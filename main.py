import asyncio
import json
import os
import re
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter

SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

MAX_ITEMS_PER_REGULATOR = int(os.getenv("MAX_ITEMS_PER_REGULATOR", "10"))
INIT_MODE = os.getenv("INIT_MODE", "0") == "1"

UA = "Mozilla/5.0 (compatible; NPA-Monitor/4.0)"

DOC_ID_RE = re.compile(r"^https?://publication\.pravo\.gov\.ru/document/\d{16}$", re.I)

def now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def stable_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:24]

def load_sources() -> List[dict]:
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"seen": {}}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("seen", {})
    return data

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

async def fetch(client: httpx.AsyncClient, url: str) -> Tuple[int, str, str, Dict[str, str]]:
    r = await client.get(url, follow_redirects=True, timeout=45.0)
    ctype = (r.headers.get("content-type") or "").lower()
    return r.status_code, r.text, ctype, dict(r.headers)

def chunk_message(text: str, limit: int = 3900) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > limit:
            parts.append(cur)
            cur = line
        else:
            cur = (cur + "\n" + line) if cur else line
    if cur:
        parts.append(cur)
    return parts

async def safe_send(bot: Bot, text: str) -> None:
    while True:
        try:
            await bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)
            return
        except RetryAfter as e:
            await asyncio.sleep(int(getattr(e, "retry_after", 5)) + 1)

def extract_pravogov_doc_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        if DOC_ID_RE.match(abs_url):
            links.append(abs_url)
    # дедуп
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:80]

async def get_pravogov_title(client: httpx.AsyncClient, doc_url: str) -> str:
    # открываем карточку документа и берем заголовок
    status, html, ctype, _h = await fetch(client, doc_url)
    if status >= 400:
        return f"Документ {doc_url.rsplit('/', 1)[-1]}"
    soup = BeautifulSoup(html, "html.parser")
    # На карточке обычно есть title/h1
    if soup.title and soup.title.get_text(strip=True):
        t = clean(soup.title.get_text())
        # иногда title шумный — но лучше, чем “Публикация”
        return t[:240]
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return clean(h1.get_text())[:240]
    return f"Документ {doc_url.rsplit('/', 1)[-1]}"

def extract_allowlist_links(base_url: str, html: str, allow_regex: List[str]) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    compiled = [re.compile(p, re.I) for p in (allow_regex or [])]
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        if any(rx.search(abs_url) for rx in compiled):
            title = clean(a.get_text(" ")) or "Документ"
            out.append((abs_url, title[:240]))
    # дедуп по ссылке
    uniq = {}
    for u, t in out:
        uniq.setdefault(u, t)
    return list(uniq.items())[:80]

async def run_once() -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing BOT_TOKEN/CHAT_ID in env")

    bot = Bot(token=BOT_TOKEN)
    sources = load_sources()
    state = load_state()
    seen: Dict[str, str] = state["seen"]
    event_time = now_str()

    bucket: Dict[str, List[Tuple[str, str]]] = {}  # regulator -> [(title, link)]

    async with httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7"}
    ) as client:

        for s in sources:
            regulator = s.get("regulator", "Источник")
            stype = s.get("type", "pravogov_block")
            url = s.get("url", "")
            if not url:
                continue

            await asyncio.sleep(0.6)  # чтобы меньше ловить флады

            try:
                status, body, ctype, headers = await fetch(client, url)

                if status == 429:
                    ra = headers.get("retry-after")
                    wait_s = int(ra) if (ra and ra.isdigit()) else 40
                    print(f"[WARN] {regulator} {url}: 429 sleep {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue

                if status >= 400:
                    print(f"[WARN] {regulator} {url}: HTTP {status}")
                    continue

                new_items: List[Tuple[str, str]] = []

                if stype == "pravogov_block":
                    doc_links = extract_pravogov_doc_links(url, body)
                    for link in doc_links:
                        iid = stable_id(link)
                        if iid in seen:
                            continue
                        seen[iid] = event_time

                        title = await get_pravogov_title(client, link)
                        new_items.append((title, link))

                elif stype == "html_allowlist":
                    allow_regex = s.get("allow_regex", [])
                    links = extract_allowlist_links(url, body, allow_regex)
                    for link, title in links:
                        iid = stable_id(link)
                        if iid in seen:
                            continue
                        seen[iid] = event_time
                        new_items.append((title, link))

                # агрегируем по регулятору
                if new_items:
                    bucket.setdefault(regulator, [])
                    # лимит по регулятору
                    for t, l in new_items:
                        if len(bucket[regulator]) >= MAX_ITEMS_PER_REGULATOR:
                            break
                        bucket[regulator].append((t, l))

            except httpx.TimeoutException:
                print(f"[WARN] {regulator} {url}: timeout")
            except Exception as e:
                print(f"[ERROR] {regulator} {url}: {repr(e)}")

    state["seen"] = seen
    state["updated_at"] = event_time
    save_state(state)

    if INIT_MODE:
        print("[INFO] INIT_MODE enabled: not sending messages")
        return

    for regulator, items in bucket.items():
        lines = []
        for i, (title, link) in enumerate(items, 1):
            lines.append(f"{i}) {title} — {link}")

        msg = (
            "Мониторинг НПА\n"
            f"Регулятор: {regulator}\n"
            f"Дата: {event_time}\n"
            f"Новые публикации: {len(items)}\n\n"
            + "\n".join(lines)
        )

        for part in chunk_message(msg):
            await safe_send(bot, part)

if __name__ == "__main__":
    asyncio.run(run_once())
