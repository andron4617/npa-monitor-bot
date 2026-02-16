import asyncio
import json
import os
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx
import feedparser
from bs4 import BeautifulSoup
from telegram import Bot

SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

UA = "Mozilla/5.0 (compatible; NPA-Monitor/1.0; +https://github.com/)"

def now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")

def load_sources() -> List[Dict[str, str]]:
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def load_state() -> Dict[str, Dict[str, str]]:
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: Dict[str, Dict[str, str]]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def stable_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:24]

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

async def fetch_text(client: httpx.AsyncClient, url: str) -> Tuple[int, str, str]:
    r = await client.get(url, follow_redirects=True, timeout=30.0)
    ctype = (r.headers.get("content-type") or "").lower()
    return r.status_code, r.text, ctype

def parse_rss(url: str) -> List[Dict[str, str]]:
    feed = feedparser.parse(url)
    items: List[Dict[str, str]] = []
    for e in feed.entries[:50]:
        title = clean(getattr(e, "title", ""))
        link = clean(getattr(e, "link", ""))
        published = clean(getattr(e, "published", "") or getattr(e, "updated", ""))
        uid = clean(getattr(e, "id", "")) or link or title
        items.append({
            "id": stable_id(uid),
            "title": title or "Публикация (RSS)",
            "link": link or url,
            "published": published
        })
    return items

def parse_html_links(base_url: str, html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    # Берём “осмысленные” ссылки. Для pravo.gov и регуляторов обычно хватает href на документ/карточку.
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = clean(a.get_text(" "))
        if not href or href.startswith("#"):
            continue

        # абсолютная ссылка
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            # домен из base_url
            m = re.match(r"^(https?://[^/]+)", base_url)
            if m:
                href = m.group(1) + href

        # фильтр “похож на документ/публикацию”
        h = href.lower()
        if any(x in h for x in ["/documents/", "/document/", "doc", "pravo.gov.ru/documents", "publication.pravo.gov.ru"]):
            links.append((href, text))

    # дедуп + ограничение
    uniq: Dict[str, str] = {}
    for href, text in links:
        if href not in uniq:
            uniq[href] = text or "Публикация"

    items: List[Dict[str, str]] = []
    for href, text in list(uniq.items())[:50]:
        items.append({
            "id": stable_id(href),
            "title": text[:200],
            "link": href,
            "published": ""
        })
    return items

async def send(bot: Bot, regulator: str, title: str, link: str, published: str, event_time: str) -> None:
    msg = (
        f"Мониторинг НПА\n"
        f"Регулятор: {regulator}\n"
        f"Событие: новая публикация\n"
        f"Дата: {event_time}\n"
        f"Документ/публикация: {title}\n"
        f"Ссылка: {link}"
    )
    if published:
        msg += f"\nПубликация в источнике: {published}"
    await bot.send_message(chat_id=CHAT_ID, text=msg, disable_web_page_preview=True)

async def run_once() -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing BOT_TOKEN / CHAT_ID in env.")

    bot = Bot(token=BOT_TOKEN)
    sources = load_sources()
    state = load_state()
    event_time = now_str()

    async with httpx.AsyncClient(headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7"}) as client:
        for s in sources:
            regulator = s.get("regulator", "Источник")
            stype = s.get("type", "html_links")
            url = s.get("url", "")
            if not url:
                continue

            try:
                if stype == "rss":
                    items = parse_rss(url)
                else:
                    status, html, ctype = await fetch_text(client, url)
                    if status >= 400:
                        print(f"[WARN] {regulator} {url}: HTTP {status}")
                        continue
                    if "html" not in ctype and "text" not in ctype:
                        print(f"[WARN] {regulator} {url}: unsupported content-type {ctype}")
                        continue
                    items = parse_html_links(url, html)

                # state per-source
                src_state = state.get(url, {"seen": {}})
                seen: Dict[str, str] = src_state.get("seen", {})

                new_count = 0
                for it in items:
                    iid = it["id"]
                    if iid in seen:
                        continue
                    # новая публикация
                    await send(
                        bot=bot,
                        regulator=regulator,
                        title=it.get("title", "Публикация"),
                        link=it.get("link", url),
                        published=it.get("published", ""),
                        event_time=event_time
                    )
                    seen[iid] = event_time
                    new_count += 1

                # обновляем state
                state[url] = {
                    "seen": seen,
                    "updated_at": event_time,
                    "last_new": str(new_count)
                }

            except Exception as e:
                print(f"[ERROR] {regulator} {url}: {repr(e)}")

    save_state(state)

if __name__ == "__main__":
    asyncio.run(run_once())
