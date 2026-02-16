import os
import re
import json
import asyncio
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup


STATE_FILE = "state.json"
SOURCES_FILE = "sources.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

# Ограничители, чтобы не словить flood/timeout
HTTP_TIMEOUT_SEC = 25
MAX_CONCURRENCY = 6
MAX_DOC_TITLE_FETCH = 6  # сколько новых документов можно "дораскрыть" (подтянуть title со страницы)
TELEGRAM_MAX_LEN = 3900  # чуть меньше 4096, чтобы не резало
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NPA-Monitor/1.0"


def now_msk_str() -> str:
    # Без плясок с tz: просто покажем локальное время раннера как UTC+3 не гарантируем.
    # Если хочешь строго МСК — добавим tzdata, но для мониторинга достаточно.
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def norm_url(u: str) -> str:
    u = u.strip()
    # убрать якоря
    if "#" in u:
        u = u.split("#", 1)[0]
    return u


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "ru,en;q=0.8"}
    for attempt in range(1, 4):
        try:
            async with session.get(url, headers=headers, timeout=HTTP_TIMEOUT_SEC, allow_redirects=True) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
                return await r.text(errors="ignore")
        except Exception as e:
            if attempt == 3:
                raise
            await asyncio.sleep(1.2 * attempt)
    raise RuntimeError("Failed to fetch")


def extract_pravo_document_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        full = norm_url(full)

        # Берём только реальные карточки документов
        if re.search(r"publication\.pravo\.gov\.ru/document/\d{16,}", full):
            links.append(full)

    # дедуп
    uniq = []
    seen = set()
    for x in links:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


async def try_get_pravo_title(session: aiohttp.ClientSession, doc_url: str) -> str:
    # Пытаемся вытащить нормальный title с карточки документа
    try:
        html = await fetch_text(session, doc_url)
        soup = BeautifulSoup(html, "lxml")

        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            t = og["content"].strip()
            if t:
                return t

        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t:
                return t

        title = soup.find("title")
        if title:
            t = title.get_text(" ", strip=True)
            t = re.sub(r"\s+", " ", t).strip()
            if t:
                return t
    except Exception:
        pass

    # fallback
    m = re.search(r"/document/(\d{16,})", doc_url)
    return f"Документ {m.group(1) if m else 'publication.pravo.gov.ru'}"


def extract_cbr_doc_links(html: str, base_url: str) -> List[Tuple[str, str]]:
    """
    Возвращает список (title, url) только для "документных" ссылок ЦБ:
    - /Content/Document/File/...
    - /Crosscut/LawActs/File/... (у тебя такое уже было)
    """
    soup = BeautifulSoup(html, "lxml")
    items: List[Tuple[str, str]] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        full = norm_url(full)

        if (
            "/Content/Document/File/" in full
            or "/Crosscut/LawActs/File/" in full
        ):
            title = a.get_text(" ", strip=True)
            title = re.sub(r"\s+", " ", title).strip()
            if not title:
                title = "Документ"
            items.append((title, full))

    # дедуп по url
    uniq = []
    seen = set()
    for t, u in items:
        if u not in seen:
            seen.add(u)
            uniq.append((t, u))
    return uniq


def extract_regulation_rss(html: str) -> List[Tuple[str, str]]:
    # regulation.gov rss — это xml
    soup = BeautifulSoup(html, "xml")
    out: List[Tuple[str, str]] = []
    for it in soup.find_all("item"):
        title = it.title.get_text(strip=True) if it.title else "Проект"
        link = it.link.get_text(strip=True) if it.link else ""
        if link:
            out.append((title, norm_url(link)))
    # дедуп
    uniq = []
    seen = set()
    for t, u in out:
        if u not in seen:
            seen.add(u)
            uniq.append((t, u))
    return uniq


def format_regulator_message(regulator: str, items: List[Tuple[str, str]]) -> List[str]:
    """
    Возвращает список сообщений (если надо порезать по длине).
    """
    header = f"Мониторинг НПА\nРегулятор: {regulator}\nДата: {now_msk_str()}\nНовые публикации: {len(items)}\n"
    lines = []
    for i, (t, u) in enumerate(items, 1):
        # аккуратно: чтобы не было "пустых названий"
        t = re.sub(r"\s+", " ", (t or "").strip())
        if not t:
            t = "Документ"
        lines.append(f"{i}) {t} — {u}")

    full = header + "\n" + "\n".join(lines)

    if len(full) <= TELEGRAM_MAX_LEN:
        return [full]

    # режем на части
    msgs = []
    cur = header + "\n"
    for ln in lines:
        if len(cur) + len(ln) + 1 > TELEGRAM_MAX_LEN:
            msgs.append(cur.rstrip())
            cur = header + "\n"
        cur += ln + "\n"
    if cur.strip():
        msgs.append(cur.rstrip())
    return msgs


async def tg_send(session: aiohttp.ClientSession, text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    async with session.post(url, json=payload, timeout=HTTP_TIMEOUT_SEC) as r:
        if r.status >= 400:
            body = await r.text(errors="ignore")
            raise RuntimeError(f"Telegram sendMessage HTTP {r.status}: {body[:200]}")


async def run_once() -> int:
    sources = load_json(SOURCES_FILE, [])
    state = load_json(STATE_FILE, {"seen": {}, "last_errors": {}})

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    errors: List[Tuple[str, str, str]] = []  # regulator, url, reason
    notifications: List[Tuple[str, List[Tuple[str, str]]]] = []  # regulator -> items

    async with aiohttp.ClientSession() as session:

        async def process_source(src: Dict[str, Any]) -> None:
            regulator = src.get("regulator", "Неизвестно")
            kind = src.get("kind", "html_links")
            urls = src.get("urls", [])
            max_items = int(src.get("max_items", 15))

            src_seen: set = set(state["seen"].get(regulator, []))

            all_new: List[Tuple[str, str]] = []

            for url in urls:
                url = norm_url(url)
                try:
                    async with sem:
                        html = await fetch_text(session, url)

                    if kind in ("pravo_block", "pravo_foiv"):
                        doc_links = extract_pravo_document_links(html, url)
                        doc_links = doc_links[:max_items]

                        # Для части ссылок подтянем нормальный title, чтобы было не "Документ"
                        new_links = [u for u in doc_links if u not in src_seen]
                        titled: List[Tuple[str, str]] = []

                        for u in new_links[:MAX_DOC_TITLE_FETCH]:
                            t = await try_get_pravo_title(session, u)
                            titled.append((t, u))
                        for u in new_links[MAX_DOC_TITLE_FETCH:]:
                            titled.append(("Документ", u))

                        all_new.extend(titled)

                    elif kind == "cbr_docs":
                        items = extract_cbr_doc_links(html, url)
                        items = items[:max_items]
                        for t, u in items:
                            if u not in src_seen:
                                all_new.append((t, u))

                    elif kind == "regulation_rss":
                        items = extract_regulation_rss(html)
                        items = items[:max_items]
                        for t, u in items:
                            if u not in src_seen:
                                all_new.append((t, u))

                    else:
                        # fallback: не используем сейчас, чтобы не тащить мусор
                        pass

                except Exception as e:
                    errors.append((regulator, url, f"{type(e).__name__}: {e}"))

            # обновим state и подготовим уведомление
            if all_new:
                # дедуп внутри источника
                uniq = []
                seen_u = set()
                for t, u in all_new:
                    if u not in seen_u:
                        seen_u.add(u)
                        uniq.append((t, u))

                notifications.append((regulator, uniq))

                # записываем что увидели (храним ограниченно, чтобы state не разрастался)
                new_seen = list(dict.fromkeys(list(src_seen) + [u for _, u in uniq]))
                state["seen"][regulator] = new_seen[-200:]

        # 1) обработка источников
        await asyncio.gather(*(process_source(s) for s in sources))

        # 2) отправка уведомлений: один регулятор = одно(или несколько по длине) сообщение
        # сортируем, чтобы порядок был стабильный
        notifications.sort(key=lambda x: x[0])

        for regulator, items in notifications:
            msgs = format_regulator_message(regulator, items)
            for m in msgs:
                await tg_send(session, m)
                await asyncio.sleep(0.7)  # анти-flood

        # 3) ошибки: агрегируем в одно сообщение и не повторяем одинаковые
        if errors:
            # сгруппируем
            err_lines = []
            for reg, url, reason in errors:
                err_lines.append(f"- {reg}: {url}\n  Причина: {reason}")
            err_text = "Мониторинг НПА\nОшибки источников:\n" + "\n".join(err_lines)

            err_hash = sha1(err_text)
            prev_hash = state.get("last_errors", {}).get("hash", "")
            if err_hash != prev_hash:
                # отправим один раз на изменение
                try:
                    await tg_send(session, err_text[:TELEGRAM_MAX_LEN])
                except Exception:
                    pass
                state["last_errors"] = {"hash": err_hash, "at": now_msk_str()}

    save_json(STATE_FILE, state)
    return 0


if __name__ == "__main__":
    # Важно: GitHub Actions запускает это как одиночный job.
    # Поэтому делаем один проход и выходим.
    asyncio.run(run_once())
