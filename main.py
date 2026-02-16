import asyncio
import json
import os
import re
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin

import httpx
import feedparser
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter

SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

# 1 сообщение на регулятора, внутри список
MAX_ITEMS_PER_REGULATOR = int(os.getenv("MAX_ITEMS_PER_REGULATOR", "15"))

# Режим инициализации: ничего не отправляем, только запоминаем.
# Используется один раз, чтобы не было “заливки” при первом запуске.
INIT_MODE = os.getenv("INIT_MODE", "0") == "1"

UA = "Mozilla/5.0 (compatible; NPA-Monitor/2.0; +https://github.com/)"

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
    if "seen" not in data:
        data["seen"] = {}
    return data

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

async def fetch(client: httpx.AsyncClient, url: str) -> Tuple[int, str, str]:
    r = await client.get(url, follow_redirects=True, timeout=30.0)
    ctype = (r.headers.get("content-type") or "").lower()
    return r.status_code, r.text, ctype

def parse_rss_from_text(rss_text: str, base_url: str) -> List[dict]:
    feed = feedparser.parse(rss_text)
    items: List[dict] = []
    for e in feed.entries[:50]:
        title = clean(getattr(e, "title", "")) or "Публикация (RSS)"
        link = clean(getattr(e, "link", "")) or base_url
        published = clean(getattr(e, "published", "") or getattr(e, "updated", ""))
        uid = clean(getattr(e, "id", "")) or link or title
        items.append({
            "id": stable_id(uid),
            "title": title[:200],
            "link": link,
            "published": published
        })
    return items

def parse_html_links(base_url: str, html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out: Dict[str, str] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = clean(a.get_text(" "))

        if not href or href.startswith("#"):
            continue

        abs_url = urljoin(base_url, href)
        h = abs_url.lower()

        # Стараемся цеплять “документные” ссылки (для pravo.gov и регуляторов)
        if ("publication.pravo.gov.ru" in h) or ("/documents/" in h) or ("/document/" in h) or ("?doc" in h) or ("file" in h):
            if abs_url not in out:
                out[abs_url] = text or "Публикация"

    items = []
    for link, title in list(out.items())[:80]:
        items.append({
            "id": stable_id(link),
            "title": title[:200],
            "link": link,
            "published": ""
        })
    return items

def chunk_message(text: str, limit: int = 3900) -> List[str]:
    # Telegram лимит ~4096, держим запас
    if len(text) <= limit:
        return [text]
    parts = []
    cur = ""
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
    # Защита от RetryAfter телеграм-флуда
    while True:
        try:
            await bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)
            return
        except RetryAfter as e:
            await asyncio.sleep(int(getattr(e, "retry_after", 5)) + 1)

async def run_once() -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing BOT_TOKEN/CHAT_ID in env")

    bot = Bot(token=BOT_TOKEN)
    sources = load_sources()
    state = load_state()
    seen: Dict[str, str] = state.get("seen", {})
    event_time = now_str()

    # Сбор новостей по регуляторам: 1 сообщение на регулятора
    bucket: Dict[str, List[dict]] = {}

    async with httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7"}
    ) as client:

        for s in sources:
            regulator = s.get("regulator", "Источник")
            stype = s.get("type", "html_links")
            url = s.get("url", "")
            if not url:
                continue

            try:
                items: List[dict] = []
                if stype == "rss":
                    status, text, _ctype = await fetch(client, url)
                    if status >= 400:
                        print(f"[WARN] {regulator} {url}: HTTP {status}")
                        continue
                    items = parse_rss_from_text(text, url)
                else:
                    status, html, ctype = await fetch(client, url)
                    if status >= 400:
                        print(f"[WARN] {regulator} {url}: HTTP {status}")
                        continue
                    if ("html" not in ctype) and ("text" not in ctype):
                        print(f"[WARN] {regulator} {url}: unsupported content-type {ctype}")
                        continue
                    items = parse_html_links(url, html)

                # Находим только новое (по глобальному seen)
                new_items = []
                for it in items:
                    iid = it["id"]
                    if iid in seen:
                        continue
                    seen[iid] = event_time
                    new_items.append(it)

                if new_items:
                    # дедуп по ссылке внутри регулятора
                    bucket.setdefault(regulator, [])
                    existing_links = {x.get("link") for x in bucket[regulator]}
                    for it in new_items:
                        if it.get("link") in existing_links:
                            continue
                        bucket[regulator].append(it)
                        existing_links.add(it.get("link"))
                        if len(bucket[regulator]) >= MAX_ITEMS_PER_REGULATOR:
                            break

            except httpx.TimeoutException:
                print(f"[WARN] {regulator} {url}: timeout")
            except Exception as e:
                print(f"[ERROR] {regulator} {url}: {repr(e)}")

    # Сохраняем память
    state["seen"] = seen
    state["updated_at"] = event_time
    save_state(state)

    # INIT_MODE — ничего не шлём
    if INIT_MODE:
        print("[INFO] INIT_MODE enabled: not sending messages")
        return

    # Отправляем 1 сообщение на регулятора
    for regulator, items in bucket.items():
        lines = []
        for i, it in enumerate(items, 1):
            title = clean(it.get("title", "Публикация"))
            link = clean(it.get("link", ""))
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
