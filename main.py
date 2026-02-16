import os
import re
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

STATE_FILE = "state.json"
SOURCES_FILE = "sources.json"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NPA-Monitor/1.0"
PRAVO_DOC_RE = re.compile(r"^/document/\d{16}$")
PRAVO_DOC_FULL_RE = re.compile(r"^https?://publication\.pravo\.gov\.ru/document/\d{16}$")
PRAVO_PDF_RE = re.compile(r"^/file/pdf\?eoNumber=\d{16}$")

# Ограничения чтобы не улететь в flood control
GLOBAL_CONCURRENCY = 3
PER_HOST_CONCURRENCY = 2
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=25)

# На практике, многие сайты РФ могут отдавать 429/503 — ретраим аккуратно
MAX_RETRIES = 4


def now_msk_str() -> str:
    # MSK = UTC+3 без переходов
    msk = timezone.utc
    # проще: просто печатать локальное время раннера не нужно, фиксируем UTC и подписываем "по МСК" в тексте
    # но чтобы было как у тебя в примере — ставим ручной +3
    from datetime import timedelta
    msk_tz = timezone(timedelta(hours=3))
    return datetime.now(msk_tz).strftime("%d.%m.%Y %H:%M")


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    host_sems: Dict[str, asyncio.Semaphore],
) -> str:
    from urllib.parse import urlparse

    host = urlparse(url).netloc
    host_sem = host_sems.setdefault(host, asyncio.Semaphore(PER_HOST_CONCURRENCY))

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with sem, host_sem:
                async with session.get(url, headers={"User-Agent": UA}) as resp:
                    # обработка 429 с Retry-After
                    if resp.status == 429:
                        ra = resp.headers.get("Retry-After")
                        sleep_s = int(ra) if (ra and ra.isdigit()) else min(10 * attempt, 40)
                        await asyncio.sleep(sleep_s)
                        continue

                    resp.raise_for_status()
                    return await resp.text(errors="ignore")
        except Exception as e:
            last_exc = e
            await asyncio.sleep(min(2 * attempt, 8))

    raise RuntimeError(f"Fetch failed after retries: {url} ({last_exc})")


def pravo_list_url(block_or_foiv: str) -> str:
    # это именно список "публикаций" с документами
    return f"https://publication.pravo.gov.ru/documents/block/{block_or_foiv}"


def abs_url(base: str, href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return base.rstrip("/") + href
    return base.rstrip("/") + "/" + href.lstrip("/")


def extract_pravo_document_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if PRAVO_DOC_RE.match(href):
            links.append(abs_url("https://publication.pravo.gov.ru", href))
    # уникализация, сохраняя порядок
    seen = set()
    out = []
    for x in links:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extract_pravo_title_from_doc(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # 1) title
    t = (soup.title.text.strip() if soup.title and soup.title.text else "").strip()
    if t:
        # часто там "Официальное опубликование..." — это мусор, но на doc-странице обычно норм
        t = re.sub(r"\s+", " ", t)
        # вырежем хвосты типа "— Официальное опубликование"
        t = t.replace("— Официальное опубликование правовых актов", "").strip(" -—")
        if len(t) >= 10:
            return t

    # 2) h1
    h1 = soup.find("h1")
    if h1 and h1.text:
        return re.sub(r"\s+", " ", h1.text.strip())

    # 3) meta og:title
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()

    return "Документ"


def is_rss(text: str) -> bool:
    t = text.lstrip()
    return t.startswith("<?xml") and ("<rss" in t or "<feed" in t)


def parse_rss_items(xml_text: str) -> List[Tuple[str, str, Optional[str]]]:
    # returns list of (title, link, pubdate_iso)
    soup = BeautifulSoup(xml_text, "xml")
    items = []

    # RSS
    for it in soup.find_all("item"):
        title = (it.title.text.strip() if it.title else "").strip() or "Документ"
        link = (it.link.text.strip() if it.link else "").strip()
        pub = None
        if it.pubDate and it.pubDate.text:
            try:
                pub = dtparser.parse(it.pubDate.text.strip()).isoformat()
            except Exception:
                pub = None
        if link:
            items.append((title, link, pub))

    # Atom
    if not items:
        for e in soup.find_all("entry"):
            title = (e.title.text.strip() if e.title else "").strip() or "Документ"
            link = ""
            l = e.find("link")
            if l and l.get("href"):
                link = l["href"].strip()
            pub = None
            d = e.find("updated") or e.find("published")
            if d and d.text:
                try:
                    pub = dtparser.parse(d.text.strip()).isoformat()
                except Exception:
                    pub = None
            if link:
                items.append((title, link, pub))

    return items


def normalize_regulator_name(name: str) -> str:
    # Никаких "ФОИВ"
    mapping = {
        "ЦБ РФ": "Банк России",
        "ФСТЭК": "ФСТЭК России",
        "ФСБ": "ФСБ России",
    }
    return mapping.get(name, name)


async def fetch_pravo_updates(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    host_sems: Dict[str, asyncio.Semaphore],
    regulator: str,
    block_or_foiv: str,
    limit: int,
    state: Dict[str, Any],
) -> List[Dict[str, str]]:
    list_html = await fetch_text(session, pravo_list_url(block_or_foiv), sem, host_sems)
    doc_links = extract_pravo_document_links(list_html)[: max(limit, 1)]

    key = f"pravo:{block_or_foiv}"
    seen = set(state.get(key, []))

    new_items = []
    for link in doc_links:
        if link in seen:
            continue
        try:
            doc_html = await fetch_text(session, link, sem, host_sems)
            title = extract_pravo_title_from_doc(doc_html)
        except Exception:
            title = "Документ"
        new_items.append({"title": title, "url": link})

    # обновляем state: храним последние 500 чтобы не раздувать
    updated = list(dict.fromkeys(doc_links + list(seen)))[:500]
    state[key] = updated
    return new_items


async def fetch_rss_updates(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    host_sems: Dict[str, asyncio.Semaphore],
    regulator: str,
    url: str,
    limit: int,
    state: Dict[str, Any],
) -> List[Dict[str, str]]:
    xml_text = await fetch_text(session, url, sem, host_sems)
    items = parse_rss_items(xml_text)[: max(limit, 1)]

    key = f"rss:{url}"
    seen = set(state.get(key, []))

    new_items = []
    current_links = []
    for title, link, _pub in items:
        current_links.append(link)
        if link in seen:
            continue
        new_items.append({"title": re.sub(r"\s+", " ", title).strip(), "url": link})

    state[key] = list(dict.fromkeys(current_links + list(seen)))[:500]
    return new_items


async def fetch_cbr_html_updates(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    host_sems: Dict[str, asyncio.Semaphore],
    url: str,
    limit: int,
    state: Dict[str, Any],
) -> List[Dict[str, str]]:
    html = await fetch_text(session, url, sem, host_sems)
    soup = BeautifulSoup(html, "lxml")

    # Берем только документы, которые выглядят как файлы/нпа, и игнорим “статистику/репо/прочее”
    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        text = re.sub(r"\s+", " ", (a.get_text(" ", strip=True) or "")).strip()
        if not href:
            continue

        full = abs_url("https://cbr.ru", href)

        # Сигнатуры документов:
        if "/Content/Document/File/" in full or "/Crosscut/LawActs/File/" in full:
            if text and len(text) >= 8:
                candidates.append((text, full))

        # Иногда ссылки на карточки НПА внутри /na/ могут быть относительные
        if "/na/" in url and "/na/" in full and ("File" in full or full.endswith("/")):
            # оставим как есть, но фильтруем по тексту
            if text and len(text) >= 8:
                candidates.append((text, full))

    # unique keep order
    seen_local = set()
    uniq = []
    for t, l in candidates:
        if l in seen_local:
            continue
        seen_local.add(l)
        uniq.append((t, l))

    uniq = uniq[: max(limit, 1)]

    key = f"cbr:{url}"
    seen = set(state.get(key, []))

    new_items = []
    current = []
    for t, l in uniq:
        current.append(l)
        if l in seen:
            continue
        new_items.append({"title": t, "url": l})

    state[key] = list(dict.fromkeys(current + list(seen)))[:500]
    return new_items


async def send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    api = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as s:
        async with s.post(api, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Telegram sendMessage failed: {resp.status} {body}")


def format_regulator_message(regulator: str, items: List[Dict[str, str]]) -> str:
    reg = normalize_regulator_name(regulator)
    lines = []
    lines.append("Мониторинг НПА")
    lines.append(f"Регулятор: {reg}")
    lines.append(f"Дата: {now_msk_str()}")
    lines.append(f"Новые публикации: {len(items)}")
    lines.append("")
    for i, it in enumerate(items, 1):
        title = it["title"].strip()
        url = it["url"].strip()
        lines.append(f"{i}) {title} — {url}")
    return "\n".join(lines)


async def run_once() -> None:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    chat_id = os.getenv("CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError("BOT_TOKEN/CHAT_ID not set in env (GitHub Secrets).")

    sources = load_json(SOURCES_FILE, [])
    state = load_json(STATE_FILE, {})

    sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)
    host_sems: Dict[str, asyncio.Semaphore] = {}

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        for src in sources:
            if not src.get("enabled", True):
                continue

            regulator = src.get("regulator", "Неизвестно")
            stype = src.get("type")
            limit = int(src.get("limit", 15))

            try:
                new_items: List[Dict[str, str]] = []
                if stype == "pravo_block":
                    block = src["block"]
                    new_items = await fetch_pravo_updates(
                        session, sem, host_sems, regulator, block, limit, state
                    )

                elif stype == "pravo_foiv":
                    foiv = src["foiv"]
                    new_items = await fetch_pravo_updates(
                        session, sem, host_sems, regulator, foiv, limit, state
                    )

                elif stype == "rss":
                    url = src["url"]
                    new_items = await fetch_rss_updates(
                        session, sem, host_sems, regulator, url, limit, state
                    )

                elif stype in ("cbr_lawacts", "cbr_projects"):
                    url = src["url"]
                    new_items = await fetch_cbr_html_updates(
                        session, sem, host_sems, url, limit, state
                    )

                elif stype == "html_generic":
                    # пока заглушка: иначе будет шум. Включать только когда будет нормальный источник.
                    continue

                else:
                    continue

                if new_items:
                    # 1 сообщение на регулятор
                    msg = format_regulator_message(regulator, new_items)
                    await send_telegram(bot_token, chat_id, msg)

            except Exception as e:
                # Ошибки не шлем в ТГ (иначе тоже будет “спам”).
                # Печатаем в логи Actions.
                print(f"[ERROR] {regulator}: {e}")

    save_json(STATE_FILE, state)


if __name__ == "__main__":
    asyncio.run(run_once())
