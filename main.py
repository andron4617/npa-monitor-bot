import os
import re
import json
import time
import uuid
import hashlib
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup


SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
GIGACHAT_AUTH_KEY = os.getenv("GIGACHAT_AUTH_KEY", "").strip()

USE_GIGACHAT = bool(GIGACHAT_AUTH_KEY)

HTTP_TIMEOUT = 30
MAX_ITEMS_PER_REGULATOR = 15

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/121 Safari/537.36"


# -----------------------------
# Utilities
# -----------------------------

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def now_str():
    return time.strftime("%d.%m.%Y %H:%M", time.gmtime())


def stable_id(regulator, title, url):
    raw = f"{regulator}|{title}|{url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


# -----------------------------
# GigaChat Production Client
# -----------------------------

GIGACHAT_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_CHAT_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"


@dataclass
class GigaToken:
    token: str
    expires: float


class GigaChatClient:
    def __init__(self, auth_key):
        self.auth_key = auth_key.replace("Basic ", "")
        self.token: Optional[GigaToken] = None
        self.lock = asyncio.Lock()

    async def _fetch_token(self, session):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "RqUID": str(uuid.uuid4()),
            "Authorization": f"Basic {self.auth_key}",
            "User-Agent": UA,
        }
        data = {"scope": "GIGACHAT_API_PERS"}

        async with session.post(GIGACHAT_OAUTH_URL, headers=headers, data=data, timeout=HTTP_TIMEOUT) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"GigaChat OAuth error {r.status}: {text}")
            js = json.loads(text)
            token = js["access_token"]
            expires = time.time() + int(js["expires_in"]) - 60
            return GigaToken(token, expires)

    async def get_token(self, session):
        async with self.lock:
            if self.token and time.time() < self.token.expires:
                return self.token.token
            self.token = await self._fetch_token(session)
            return self.token.token

    async def classify(self, session, regulator, title, url):
        token = await self.get_token(session)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        prompt = (
            "Ответь строго YES или NO.\n"
            "YES если документ относится к информационной безопасности, КИИ, ПДн, криптографии, инцидентам, антифроду.\n"
            "NO если это статистика, ставки, вакансии, контакты.\n\n"
            f"Регулятор: {regulator}\n"
            f"Заголовок: {title}\n"
            f"Ссылка: {url}\n"
            "Ответ:"
        )

        body = {
            "model": "GigaChat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": 3,
        }

        async with session.post(GIGACHAT_CHAT_URL, headers=headers, json=body, timeout=HTTP_TIMEOUT) as r:
            text = await r.text()
            if r.status >= 400:
                return False
            js = json.loads(text)
            ans = js["choices"][0]["message"]["content"].strip().upper()
            return ans.startswith("YES")


# -----------------------------
# Parsing
# -----------------------------

async def fetch(session, url):
    async with session.get(url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT) as r:
        text = await r.text()
        if r.status >= 400:
            raise RuntimeError(f"HTTP {r.status}")
        return text


def parse_pravo(html):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select('a[href*="/document/"]'):
        href = a.get("href")
        if href.startswith("/"):
            href = "http://publication.pravo.gov.ru" + href
        title = a.get_text(strip=True) or "Документ"
        out.append((title, href))
    return out


def parse_html_links(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if href.startswith("/"):
            base = re.match(r"^(https?://[^/]+)", base_url).group(1)
            href = base + href
        title = a.get_text(strip=True) or "Документ"
        out.append((title, href))
    return out


def parse_rss(xml):
    soup = BeautifulSoup(xml, "xml")
    out = []
    for item in soup.find_all("item"):
        title = item.findtext("title")
        link = item.findtext("link")
        if title and link:
            out.append((title, link))
    return out


# -----------------------------
# Telegram
# -----------------------------

async def send_tg(session, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}
    async with session.post(url, json=payload) as r:
        if r.status >= 400:
            raise RuntimeError("Telegram error")


def format_message(regulator, items):
    lines = []
    lines.append("Мониторинг НПА (ИБ)")
    lines.append(f"Регулятор: {regulator}")
    lines.append(f"Дата: {now_str()}")
    lines.append(f"Новые публикации: {len(items)}")
    lines.append("")
    for i, it in enumerate(items[:MAX_ITEMS_PER_REGULATOR], 1):
        lines.append(f"{i}) {it['title']} — {it['url']}")
    return "\n".join(lines)


# -----------------------------
# Main
# -----------------------------

async def main():
    sources = load_json(SOURCES_FILE, [])
    state = load_json(STATE_FILE, {"seen": {}})

    giga = GigaChatClient(GIGACHAT_AUTH_KEY) if USE_GIGACHAT else None

    async with aiohttp.ClientSession() as session:
        for src in sources:
            regulator = src["regulator"]
            html = await fetch(session, src["url"])

            if src["type"] == "pravo_block":
                raw = parse_pravo(html)
            elif src["type"] == "rss":
                raw = parse_rss(html)
            else:
                raw = parse_html_links(html, src["url"])

            new_items = []
            seen_ids = state["seen"].setdefault(src["id"], [])

            for title, link in raw:
                sid = stable_id(regulator, title, link)
                if sid in seen_ids:
                    continue

                is_infosec = False
                if giga:
                    try:
                        is_infosec = await giga.classify(session, regulator, title, link)
                    except Exception:
                        is_infosec = False
                else:
                    # fallback keyword check
                    if any(k in (title + link).lower() for k in ["безопас", "кибер", "крипто", "пдн", "информац"]):
                        is_infosec = True

                if is_infosec:
                    new_items.append({"title": title, "url": link})
                    seen_ids.append(sid)

            if new_items:
                msg = format_message(regulator, new_items)
                await send_tg(session, msg)

    save_json(STATE_FILE, state)


if __name__ == "__main__":
    asyncio.run(main())
