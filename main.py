import os
import re
import json
import time
import hashlib
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

MSK = timezone(timedelta(hours=3))
STATE_FILE = "state.json"
SOURCES_FILE = "sources.json"

MAX_NEW_PER_REGULATOR = 10
HTTP_TIMEOUT_SEC = 20
GLOBAL_CONCURRENCY = 6
PER_HOST_CONCURRENCY = 2


def now_msk_str() -> str:
    return datetime.now(MSK).strftime("%d.%m.%Y %H:%M")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_url(u: str) -> str:
    u = u.strip()
    if u.startswith("//"):
        u = "https:" + u
    return u


def is_same_host(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc == urlparse(b).netloc
    except Exception:
        return False


def pick_host(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


class Fetcher:
    def __init__(self) -> None:
        self.global_sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)
        self.host_sems: Dict[str, asyncio.Semaphore] = {}

    def _host_sem(self, url: str) -> asyncio.Semaphore:
        host = pick_host(url)
        if host not in self.host_sems:
            self.host_sems[host] = asyncio.Semaphore(PER_HOST_CONCURRENCY)
        return self.host_sems[host]

    async def get_text(self, session: aiohttp.ClientSession, url: str) -> str:
        url = normalize_url(url)
        # ретраи + backoff
        backoff = 2
        for attempt in range(1, 5):
            try:
                async with self.global_sem, self._host_sem(url):
                    async with session.get(url, timeout=HTTP_TIMEOUT_SEC) as r:
                        if r.status in (429, 503):
                            # rate limit / временно недоступно
                            retry_after = r.headers.get("Retry-After")
                            wait_s = int(retry_after) if retry_after and retry_after.isdigit() else backoff
                            await asyncio.sleep(wait_s)
                            backoff = min(backoff * 2, 20)
                            continue
                        r.raise_for_status()
                        return await r.text(errors="ignore")
            except asyncio.TimeoutError:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 20)
            except Exception:
                if attempt == 4:
                    raise
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 20)
        raise RuntimeError(f"Failed to fetch {url}")


async def send_telegram_message(session: aiohttp.ClientSession, bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    async with session.post(url, json=payload, timeout=HTTP_TIMEOUT_SEC) as r:
        r.raise_for_status()


def parse_rss_items(xml_text: str) -> List[Tuple[str, str]]:
    # (title, link)
    soup = BeautifulSoup(xml_text, "xml")
    items = []
    for it in soup.find_all("item"):
        title = (it.find("title").get_text(strip=True) if it.find("title") else "").strip()
        link = (it.find("link").get_text(strip=True) if it.find("link") else "").strip()
        if title and link:
            items.append((title, link))
    # Atom fallback
    if not items:
        soup = BeautifulSoup(xml_text, "xml")
        for entry in soup.find_all("entry"):
            title = (entry.find("title").get_text(strip=True) if entry.find("title") else "").strip()
            link_tag = entry.find("link")
            link = ""
            if link_tag and link_tag.get("href"):
                link = link_tag.get("href").strip()
            if title and link:
                items.append((title, link))
    return items


PRAVO_DOC_RE = re.compile(r"^https?://publication\.pravo\.gov\.ru/document/\d{16}$")


def extract_pravo_document_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if PRAVO_DOC_RE.match(href):
            links.append(href)
    # уникализируем, сохраняя порядок
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_html_links(html: str, base_url: str) -> List[Tuple[str, str]]:
    """
    Возвращает (title, url) по ссылкам на странице.
    Фильтруем мусор: якоря, mailto, js.
    """
    soup = BeautifulSoup(html, "lxml")
    items: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        full = urljoin(base_url, href)
        full = normalize_url(full)
        title = a.get_text(" ", strip=True)
        title = re.sub(r"\s+", " ", title).strip()
        if not full or len(full) < 10:
            continue
        if not title:
            title = "Документ"
        items.append((title, full))

    # чистим дубли
    seen = set()
    out = []
    for t, u in items:
        k = (t, u)
        if k not in seen:
            seen.add(k)
            out.append((t, u))
    return out


def group_by_regulator(items: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for it in items:
        grouped.setdefault(it["regulator"], []).append(it)
    return grouped


def format_regulator_message(regulator: str, items: List[Dict[str, str]]) -> str:
    lines = []
    lines.append("Мониторинг НПА")
    lines.append(f"Регулятор: {regulator}")
    lines.append(f"Дата: {now_msk_str()}")
    lines.append(f"Новые публикации: {len(items)}")
    lines.append("")
    for i, it in enumerate(items, 1):
        title = it.get("title", "Документ").strip()
        link = it.get("link", "").strip()
        # чтобы не было “огромных простыней”
        if len(title) > 240:
            title = title[:237] + "…"
        lines.append(f"{i}) {title} — {link}")
    return "\n".join(lines)


async def run_once() -> None:
    bot_token = os.environ.get("BOT_TOKEN", "").strip()
    chat_id = os.environ.get("CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError("BOT_TOKEN/CHAT_ID are not set (GitHub Secrets).")

    sources = load_json_file(SOURCES_FILE, [])
    state = load_json_file(STATE_FILE, {"seen": {}})

    if "seen" not in state or not isinstance(state["seen"], dict):
        state = {"seen": {}}

    fetcher = Fetcher()

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NPA-Monitor/1.0; +https://github.com/)"
    }

    new_items: List[Dict[str, str]] = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for src in sources:
            regulator = src.get("regulator", "Неизвестно").strip()
            url = src.get("url", "").strip()
            src_type = src.get("type", "").strip()
            src_id = src.get("id", sha1(regulator + url))[:24]

            seen_set = set(state["seen"].get(src_id, []))

            try:
                text = await fetcher.get_text(session, url)

                extracted: List[Tuple[str, str]] = []

                if src_type == "rss":
                    extracted = parse_rss_items(text)

                elif src_type == "pravo_block":
                    doc_links = extract_pravo_document_links(text)
                    # Название документа берём как "Документ" (быстро),
                    # а детали потом можно докрутить отдельным fetch на карточку.
                    extracted = [("Документ", u) for u in doc_links]

                elif src_type == "html_links":
                    extracted = extract_html_links(text, url)

                else:
                    # неизвестный тип - пропускаем
                    continue

                # дедуп + лимит
                added = 0
                for title, link in extracted:
                    link = normalize_url(link)
                    key = sha1(link)
                    if key in seen_set:
                        continue
                    seen_set.add(key)
                    new_items.append({"regulator": regulator, "title": title, "link": link})
                    added += 1
                    if added >= MAX_NEW_PER_REGULATOR:
                        break

                # сохраняем обновлённое seen для источника
                state["seen"][src_id] = list(seen_set)[-5000:]  # ограничим рост

                # уважим сайты — маленькая пауза
                await asyncio.sleep(0.4)

            except Exception as e:
                # Не валим весь прогон из-за одного источника
                err_text = f"Мониторинг НПА\nРегулятор: {regulator}\nДата: {now_msk_str()}\nОшибка источника: {url}\nПричина: {type(e).__name__}: {e}"
                await send_telegram_message(session, bot_token, chat_id, err_text)
                continue

        # Группируем и отправляем одним сообщением на регулятора
        grouped = group_by_regulator(new_items)

        for regulator, items in grouped.items():
            msg = format_regulator_message(regulator, items)
            await send_telegram_message(session, bot_token, chat_id, msg)
            await asyncio.sleep(0.6)

    save_json_file(STATE_FILE, state)


if __name__ == "__main__":
    asyncio.run(run_once())
