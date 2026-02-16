import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup
from dateutil import tz
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole

TZ_MSK = tz.gettz("Europe/Moscow")

STATE_FILE = "state.json"
SOURCES_FILE = "sources.json"

MAX_ITEMS_PER_REGULATOR = 15      # максимум в одном сообщении на регулятора
MAX_TOTAL_ITEMS = 60              # общий лимит, чтобы не улетать в flood/timeout
HTTP_TIMEOUT = 25
CONCURRENCY = 4

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) npa-monitor-bot/1.0"

IB_SYSTEM_PROMPT = """Ты — эксперт по информационной безопасности и регуляторике РФ.
Твоя задача: отбирать ТОЛЬКО документы, которые относятся к ИБ/кибербезопасности/персональным данным/КИИ/криптографии/платежной безопасности/антифроду/ИБ в финсекторе.

Верни СТРОГО JSON (без текста вокруг) формата:
{
  "relevant": true/false,
  "score": 0..100,
  "reason": "коротко (до 200 символов)",
  "tags": ["...","..."]
}

Правила:
- Если документ про финансы, статистику, РЕПО, вакансии, контакты, новости общего плана — irrelevant.
- Если это проект НПА/приказ/указ/стандарт/методика/требования и есть связь с ИБ — relevant.
"""

@dataclass
class Item:
    regulator: str
    title: str
    url: str
    published: Optional[str] = None
    source_url: Optional[str] = None


def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def now_msk_str() -> str:
    return datetime.now(tz=TZ_MSK).strftime("%d.%m.%Y %H:%M")


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def is_probably_navigation(title: str, url: str) -> bool:
    t = title.lower()
    bad_words = [
        "контак", "карта сайта", "sitemap", "ваканс", "пресс-центр", "press",
        "новости", "news", "обратиться", "о сайте", "en", "eng", "тел", "rutube", "vk.com", "ok.ru", "t.me/"
    ]
    if any(w in t for w in bad_words):
        return True
    if url.startswith("tel:"):
        return True
    return False


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()
        return await r.text(errors="ignore")


async def fetch_bytes(session: aiohttp.ClientSession, url: str) -> bytes:
    async with session.get(url, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()
        return await r.read()


async def robust_fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    # backoff на 429/5xx и на сетевые ошибки
    delays = [0, 2, 5, 10]
    last_err = None
    for d in delays:
        if d:
            await asyncio.sleep(d)
        try:
            return await fetch_text(session, url)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to fetch {url}") from last_err


def extract_pravo_doc_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href*='/document/']"):
        href = a.get("href", "")
        if href.startswith("/"):
            href = "http://publication.pravo.gov.ru" + href
        if href.startswith("http"):
            links.append(href)
    # уникализируем, сохраняем порядок
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


async def get_pravo_title(session: aiohttp.ClientSession, doc_url: str) -> str:
    html = await robust_fetch_text(session, doc_url)
    soup = BeautifulSoup(html, "lxml")
    # На pravo обычно есть meta og:title или h1
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return norm_space(og["content"])
    h1 = soup.find("h1")
    if h1:
        return norm_space(h1.get_text(" "))
    title = soup.title.get_text(" ") if soup.title else "Документ"
    return norm_space(title)


def parse_rss_items(xml_text: str) -> List[Tuple[str, str, Optional[str]]]:
    # возвращает (title, link, pubDate)
    soup = BeautifulSoup(xml_text, "xml")
    out = []
    for it in soup.find_all("item"):
        title = norm_space(it.title.get_text()) if it.title else "Документ"
        link = norm_space(it.link.get_text()) if it.link else ""
        pub = norm_space(it.pubDate.get_text()) if it.pubDate else None
        if link:
            out.append((title, link, pub))
    return out


async def collect_items(session: aiohttp.ClientSession, src: Dict[str, Any]) -> List[Item]:
    reg = src["regulator"]
    typ = src["type"]
    url = src["url"]

    items: List[Item] = []

    if typ == "pravo_block":
        html = await robust_fetch_text(session, url)
        doc_links = extract_pravo_doc_links(html)[:MAX_ITEMS_PER_REGULATOR]
        for dl in doc_links:
            try:
                t = await get_pravo_title(session, dl)
            except Exception:
                t = "Документ"
            items.append(Item(regulator=reg, title=t, url=dl, source_url=url))
        return items

    if typ == "pravo_rss":
        # общий RSS, дальше фильтруем по block через url, если возможно
        # publication.pravo.gov.ru/rss — отдаёт общий поток; block может не поддерживаться стабильно
        # поэтому берём RSS и фильтруем по наличию сегмента /document/ и по ключам regulator в тексте заголовка
        xml = await robust_fetch_text(session, url)
        rss = parse_rss_items(xml)
        # лёгкая фильтрация: оставим только документы, остальное отрежем
        for title, link, pub in rss:
            if "/document/" not in link:
                continue
            items.append(Item(regulator=reg, title=title, url=link, published=pub, source_url=url))
        return items[:MAX_ITEMS_PER_REGULATOR]

    if typ == "cbr_rss":
        xml = await robust_fetch_text(session, url)
        rss = parse_rss_items(xml)
        for title, link, pub in rss[:MAX_ITEMS_PER_REGULATOR]:
            items.append(Item(regulator=reg, title=title, url=link, published=pub, source_url=url))
        return items

    if typ == "cbr_html":
        html = await robust_fetch_text(session, url)
        soup = BeautifulSoup(html, "lxml")
        # Берём ссылки на файлы/документы и страницы проекта
        links = []
        for a in soup.select("a[href]"):
            href = a["href"]
            if not href:
                continue
            if href.startswith("/"):
                href = "https://cbr.ru" + href
            text = norm_space(a.get_text(" "))
            if not text:
                continue
            if any(x in href.lower() for x in ["/content/document/file/", "/project_na", "/crosscut/lawacts/file/"]):
                links.append((text, href))
        seen = set()
        for t, u in links:
            if u in seen or is_probably_navigation(t, u):
                continue
            seen.add(u)
            items.append(Item(regulator=reg, title=t, url=u, source_url=url))
            if len(items) >= MAX_ITEMS_PER_REGULATOR:
                break
        return items

    if typ == "regulation_rss":
        xml = await robust_fetch_text(session, url)
        rss = parse_rss_items(xml)
        for title, link, pub in rss[:MAX_ITEMS_PER_REGULATOR]:
            items.append(Item(regulator=reg, title=title, url=link, published=pub, source_url=url))
        return items

    if typ == "nspk_security":
        html = await robust_fetch_text(session, url)
        soup = BeautifulSoup(html, "lxml")
        links = []
        for a in soup.select("a[href]"):
            href = a["href"]
            text = norm_space(a.get_text(" "))
            if href.startswith("/"):
                href = "https://www.nspk.ru" + href
            if not href.startswith("http"):
                continue
            if is_probably_navigation(text, href):
                continue
            # оставляем только security-разделы и документы/страницы стандартов
            if "/cards-mir/security" in href or "pcidss" in href.lower() or "pci" in text.lower():
                links.append((text or "Документ", href))
        seen = set()
        for t, u in links:
            if u in seen:
                continue
            seen.add(u)
            items.append(Item(regulator=reg, title=t, url=u, source_url=url))
            if len(items) >= MAX_ITEMS_PER_REGULATOR:
                break
        return items

    if typ == "rosstandard":
        html = await robust_fetch_text(session, url)
        soup = BeautifulSoup(html, "lxml")
        links = []
        for a in soup.select("a[href]"):
            href = a["href"]
            text = norm_space(a.get_text(" "))
            if href.startswith("/"):
                href = "https://www.rst.gov.ru" + href
            if not href.startswith("http"):
                continue
            if is_probably_navigation(text, href):
                continue
            # на старте берём только то, что похоже на документы/новости/стандарты
            if any(k in href.lower() for k in ["gost", "standard", "docs", "document", "news"]):
                links.append((text or "Документ", href))
        seen = set()
        for t, u in links:
            if u in seen:
                continue
            seen.add(u)
            items.append(Item(regulator=reg, title=t, url=u, source_url=url))
            if len(items) >= MAX_ITEMS_PER_REGULATOR:
                break
        return items

    # неизвестный тип
    return items


def item_key(it: Item) -> str:
    return it.url


def load_state() -> Dict[str, Any]:
    state = load_json(STATE_FILE)
    if not isinstance(state, dict):
        return {"seen": {}}
    state.setdefault("seen", {})
    return state


def mark_seen(state: Dict[str, Any], it: Item) -> None:
    reg = it.regulator
    state["seen"].setdefault(reg, {})
    state["seen"][reg][item_key(it)] = {"ts": datetime.now(timezone.utc).isoformat()}


def is_seen(state: Dict[str, Any], it: Item) -> bool:
    reg = it.regulator
    return item_key(it) in state.get("seen", {}).get(reg, {})


async def tg_send(session: aiohttp.ClientSession, token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    async with session.post(url, json=payload, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()


def build_message(regulator: str, items: List[Item]) -> str:
    lines = []
    lines.append("Мониторинг НПА (ИБ)")
    lines.append(f"Регулятор: {regulator}")
    lines.append(f"Дата: {now_msk_str()}")
    lines.append(f"Новые публикации: {len(items)}")
    lines.append("")
    for i, it in enumerate(items, 1):
        lines.append(f"{i}) {it.title} — {it.url}")
    return "\n".join(lines)


def gigachat_filter(items: List[Item], credentials: str) -> List[Tuple[Item, Dict[str, Any]]]:
    # Синхронно, но мало элементов (мы режем лимитами). Для GitHub Actions это ок.
    out: List[Tuple[Item, Dict[str, Any]]] = []
    with GigaChat(credentials=credentials, verify_ssl_certs=False) as giga:
        for it in items:
            # минимальный контекст, чтобы не тратить токены
            user = {
                "title": it.title,
                "url": it.url,
                "regulator": it.regulator
            }
            chat = Chat(messages=[
                Messages(role=MessagesRole.SYSTEM, content=IB_SYSTEM_PROMPT),
                Messages(role=MessagesRole.USER, content=json.dumps(user, ensure_ascii=False))
            ])
            try:
                resp = giga.chat(chat)
                raw = resp.choices[0].message.content.strip()
                data = json.loads(raw)
                if isinstance(data, dict):
                    out.append((it, data))
            except Exception:
                # если ИИ упал — считаем нерелевантным, чтобы не слать мусор
                continue

    # оставляем только relevant + сортируем по score
    rel = [(it, meta) for it, meta in out if meta.get("relevant") is True]
    rel.sort(key=lambda x: int(x[1].get("score", 0)), reverse=True)
    return rel


async def run_once() -> None:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    chat_id = os.getenv("CHAT_ID", "").strip()
    giga_credentials = os.getenv("GIGACHAT_CREDENTIALS", "").strip()

    if not bot_token or not chat_id:
        raise RuntimeError("BOT_TOKEN/CHAT_ID are missing")
    if not giga_credentials:
        raise RuntimeError("GIGACHAT_CREDENTIALS is missing")

    sources = load_json(SOURCES_FILE)
    if not isinstance(sources, list):
        raise RuntimeError("sources.json invalid")

    state = load_state()

    connector = aiohttp.TCPConnector(ssl=False, limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    headers = {"User-Agent": UA, "Accept": "*/*"}

    all_new: List[Item] = []
    errors: List[str] = []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def one_source(src: Dict[str, Any]) -> None:
            reg = src.get("regulator", "Источник")
            url = src.get("url", "")
            try:
                async with sem:
                    items = await collect_items(session, src)
                # фильтр на “навигацию”
                items = [it for it in items if not is_probably_navigation(it.title, it.url)]
                # только новые
                new_items = [it for it in items if not is_seen(state, it)]
                for it in new_items:
                    all_new.append(it)
            except Exception as e:
                errors.append(f"{reg}: {url} -> {type(e).__name__}: {e}")

        await asyncio.gather(*(one_source(s) for s in sources))

        # ограничим общий поток, иначе будет flood и timeout
        all_new = all_new[:MAX_TOTAL_ITEMS]

        # группируем по регулятору -> фильтруем ИИ
        by_reg: Dict[str, List[Item]] = {}
        for it in all_new:
            by_reg.setdefault(it.regulator, []).append(it)

        for regulator, items in by_reg.items():
            # AI отбор
            filtered = gigachat_filter(items, giga_credentials)
            final_items = [it for it, meta in filtered][:MAX_ITEMS_PER_REGULATOR]

            if not final_items:
                # ничего ИБ-релевантного — молчим
                continue

            # отмечаем seen
            for it in final_items:
                mark_seen(state, it)

            msg = build_message(regulator, final_items)
            await tg_send(session, bot_token, chat_id, msg)

        # ошибки — одним сообщением, но без спама
        if errors:
            err_text = "Мониторинг НПА (ИБ)\nОшибки источников:\n" + "\n".join(errors[:15])
            await tg_send(session, bot_token, chat_id, err_text)

    save_json(STATE_FILE, state)


if __name__ == "__main__":
    asyncio.run(run_once())
