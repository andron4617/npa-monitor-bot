import asyncio
import hashlib
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from telegram import Bot

SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

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

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)[:180]
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)[:180]
    return "Публикация на странице источника"

async def fetch_url(client: httpx.AsyncClient, url: str) -> Tuple[int, bytes, str]:
    r = await client.get(url, follow_redirects=True, timeout=30.0)
    return r.status_code, r.content, r.headers.get("content-type", "").lower()

async def send_notice(bot: Bot, regulator: str, event: str, date_str: str, title: str, url: str) -> None:
    text = (
        f"Регулятор: {regulator}\n"
        f"Событие: {event}\n"
        f"Дата: {date_str}\n"
        f"Документ/публикация: {title}\n"
        f"Ссылка: {url}"
    )
    await bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True)

async def check_one(client: httpx.AsyncClient, regulator: str, url: str) -> Optional[Tuple[str, str]]:
    try:
        status, content, ctype = await fetch_url(client, url)
        if status >= 400:
            print(f"[WARN] {regulator} {url}: HTTP {status}")
            return None

        content_hash = sha256_hex(content)

        if "text/html" in ctype or ctype.startswith("text/"):
            html = content.decode("utf-8", errors="ignore")
            title = extract_title(html)
        else:
            title = "Документ/файл (обновление публикации)"

        return content_hash, title

    except Exception as e:
        print(f"[ERROR] {regulator} {url}: {e}")
        return None

async def run_once(bot: Bot, sources: List[Dict[str, str]], state: Dict[str, Dict[str, str]]) -> None:
    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    async with httpx.AsyncClient(headers={"User-Agent": "NPA-Monitor-Bot/1.0"}) as client:
        for s in sources:
            regulator = s["regulator"]
            url = s["url"]

            res = await check_one(client, regulator, url)
            if res is None:
                continue

            new_hash, title = res
            prev = state.get(url)

            if prev is None:
                state[url] = {"hash": new_hash, "title": title, "updated_at": date_str}
                await send_notice(bot, regulator, "зафиксирована публикация источника", date_str, title, url)
                continue

            if prev.get("hash") != new_hash:
                state[url] = {"hash": new_hash, "title": title, "updated_at": date_str}
                await send_notice(bot, regulator, "обнаружено обновление публикации", date_str, title, url)

async def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Set BOT_TOKEN and CHAT_ID environment variables.")

    sources = load_sources()
    state = load_state()

    bot = Bot(token=BOT_TOKEN)
    await run_once(bot, sources, state)
    save_state(state)

if __name__ == "__main__":
    asyncio.run(main())
