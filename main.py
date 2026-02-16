import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import ClientTimeout

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

# ---- Настройки ----
MAX_ITEMS_PER_REGULATOR = 15
HTTP_TIMEOUT_SECONDS = 25
HTTP_RETRIES = 3
HTTP_RETRY_BACKOFF = 1.6  # множитель
TELEGRAM_MAX_CHARS = 3500  # безопасно < 4096
TELEGRAM_RETRIES = 5

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# грубая фильтрация "мусорных" ссылок без ИИ/ключевых слов (как ты просил)
DROP_URL_PATTERNS = [
    r"^tel:",
    r"vk\.com/",
    r"t\.me/",
    r"youtube\.com/",
    r"rutube\.ru/",
    r"ok\.ru/",
    r"zen\.yandex\.ru/",
    r"/sitemap",
    r"/contacts",
    r"/contact",
    r"/about",
    r"/vacancies",
    r"/press",
    r"/press-center",
    r"/news(?!/)",         # оставить /news/<id> если вдруг, но выкинуть /news/ как раздел
    r"/Localization/",
    r"/SwitchLanguage",
    r"/help",
    r"/OpenData",
    r"/HtmlConstructor",
    r"/calendar/",
    r"/search/",
    r"/documents/(daily|weekly|monthly)\b",
    r"/documents/block/[^?]+(\?index=\d+)?$",  # страницы пагинации блоков (это не документ)
]

DOC_URL_PATTERNS = [
    r"https?://publication\.pravo\.gov\.ru/document/\d+",
    r"https?://cbr\.ru/Crosscut/LawActs/File/\d+",
    r"https?://cbr\.ru/Content/Document/File/\d+/.+\.pdf",
    r"https?://cbr\.ru/Content/Document/File/\d+/.+",
    r"https?://regulation\.gov\.ru/projects/\d+",
    r"https?://www\.rst\.gov\.ru/portal/gost/.*",
    r"https?://www\.nspk\.ru/.*",
    r"https?://fstec\.ru/.*",
]

PRAVO_DOC_RE = re.compile(r"https?://publication\.pravo\.gov\.ru/document/\d+")
REGULATION_PROJECT_RE = re.compile(r"https?://regulation\.gov\.ru/projects/\d+")


@dataclass(frozen=True)
class Item:
    title: str
    url: str


def now_msk_str() -> str:
    # MSK = UTC+3
    msk = timezone.utc
    # проще: взять UTC и прибавить 3 часа для отображения
    ts = datetime.utcnow().timestamp() + 3 * 3600
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")


def is_drop_url(url: str) -> bool:
    u = url.strip()
    for p in DROP_URL_PATTERNS:
        if re.search(p, u, flags=re.IGNORECASE):
            return True
    return False


def normalize_url(url: str) -> str:
    return url.strip().replace(" ", "")


def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def load_sources() -> List[Dict[str, Any]]:
    data = load_json_file(SOURCES_FILE, [])
    if not isinstance(data, list):
        raise ValueError("sources.json должен быть массивом")
    # минимальная валидация
    for i, s in enumerate(data):
        if not isinstance(s, dict) or "regulator" not in s or "urls" not in s:
            raise ValueError(f"Неверная запись в sources.json (index={i})")
        if not isinstance(s["urls"], list) or not s["urls"]:
            raise ValueError(f"Поле urls должно быть непустым массивом (index={i})")
    return data


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            async with session.get(url) as r:
                # 429 и 5xx — retry
                if r.status == 429 or 500 <= r.status <= 599:
                    txt = await r.text(errors="ignore")
                    raise RuntimeError(f"HTTP {r.status}: {txt[:300]}")
                if r.status >= 400:
                    txt = await r.text(errors="ignore")
                    raise RuntimeError(f"HTTP {r.status}: {txt[:300]}")
                return await r.text(errors="ignore")
        except Exception as e:
            last_err = e
            if attempt < HTTP_RETRIES:
                await asyncio.sleep((HTTP_RETRY_BACKOFF ** (attempt - 1)) + 0.2)
            else:
                raise
    raise RuntimeError(str(last_err))


def extract_links_from_html(base_url: str, html: str) -> List[str]:
    # очень простой парсер ссылок без bs4
    links = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        href = m.group(1).strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        links.append(abs_url)
    return links


def pick_document_links(urls: List[str]) -> List[str]:
    out = []
    seen = set()
    for u in urls:
        u = normalize_url(u)
        if not u or u in seen:
            continue
        seen.add(u)

        # убираем мусор
        if is_drop_url(u):
            continue

        # оставляем только похожее на документы/карточки
        ok = False
        for p in DOC_URL_PATTERNS:
            if re.search(p, u, flags=re.IGNORECASE):
                ok = True
                break

        # на pravo.gov.ru важнее конкретные /document/<id>
        if "publication.pravo.gov.ru" in u:
            ok = bool(PRAVO_DOC_RE.search(u))

        # на regulation.gov — только /projects/<id>
        if "regulation.gov.ru" in u:
            ok = bool(REGULATION_PROJECT_RE.search(u))

        if ok:
            out.append(u)
    return out


async def title_for_known_docs(session: aiohttp.ClientSession, url: str) -> str:
    # без OCR/тяжелого парсинга — берём <title> если получится
    try:
        html = await fetch_text(session, url)
        m = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        if m:
            title = re.sub(r"\s+", " ", m.group(1)).strip()
            # подчистить хвосты
            title = title.replace("∙", "·")
            if title:
                return title[:240]
    except Exception:
        pass
    return "Документ"


async def collect_items_for_source(
    session: aiohttp.ClientSession,
    source_name: str,
    url: str,
) -> Tuple[List[Item], Optional[str]]:
    """
    Возвращает (items, error_text)
    items — список найденных документоподобных ссылок + заголовки
    """
    try:
        html = await fetch_text(session, url)
        links = extract_links_from_html(url, html)
        doc_links = pick_document_links(links)

        # ограничение количества, чтобы не DDOSить сайты и не ловить 429
        doc_links = doc_links[:MAX_ITEMS_PER_REGULATOR * 3]

        items: List[Item] = []
        for link in doc_links:
            title = await title_for_known_docs(session, link) if "publication.pravo.gov.ru/document/" in link else "Документ"
            # regulation.gov — там обычно нормальные заголовки прямо в тексте ссылки не достанем без парсинга страницы,
            # поэтому оставляем "Документ" (потом добавим ИИ)
            if "regulation.gov.ru/projects/" in link:
                title = "Проект НПА"
            items.append(Item(title=title, url=link))

        # дедуп внутри источника
        uniq: Dict[str, Item] = {}
        for it in items:
            if it.url not in uniq:
                uniq[it.url] = it
        return list(uniq.values()), None

    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def load_state() -> Dict[str, Any]:
    st = load_json_file(STATE_FILE, {})
    if not isinstance(st, dict):
        st = {}
    if "seen" not in st or not isinstance(st.get("seen"), dict):
        st["seen"] = {}
    return st


def is_new_and_mark(state: Dict[str, Any], regulator: str, url: str) -> bool:
    seen = state["seen"].setdefault(regulator, {})
    # хранить timestamp последнего появления
    if url in seen:
        return False
    seen[url] = int(time.time())
    return True


def compact_seen(state: Dict[str, Any], keep_days: int = 45) -> None:
    cutoff = int(time.time()) - keep_days * 86400
    seen = state.get("seen", {})
    for reg in list(seen.keys()):
        reg_map = seen.get(reg, {})
        if not isinstance(reg_map, dict):
            seen[reg] = {}
            continue
        for url in list(reg_map.keys()):
            ts = reg_map.get(url, 0)
            if not isinstance(ts, int) or ts < cutoff:
                reg_map.pop(url, None)


def format_message(regulator: str, items: List[Item]) -> str:
    lines = []
    lines.append("Мониторинг НПА (ИБ)")
    lines.append(f"Регулятор: {regulator}")
    lines.append(f"Дата: {now_msk_str()}")
    lines.append(f"Новые публикации: {len(items)}")
    lines.append("")
    for i, it in enumerate(items, 1):
        lines.append(f"{i}) {it.title} — {it.url}")
    return "\n".join(lines)


async def send_tg(session: aiohttp.ClientSession, text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("BOT_TOKEN/CHAT_ID не заданы в env")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    # режем по лимиту
    chunks = []
    t = text.strip()
    while t:
        chunk = t[:TELEGRAM_MAX_CHARS]
        # пытаться резать по границе строки
        if len(t) > TELEGRAM_MAX_CHARS:
            last_nl = chunk.rfind("\n")
            if last_nl > 800:
                chunk = chunk[:last_nl]
        chunks.append(chunk)
        t = t[len(chunk):].lstrip("\n")

    for part in chunks:
        payload = {
            "chat_id": CHAT_ID,
            "text": part,
            "disable_web_page_preview": True,
        }

        last_err = None
        for attempt in range(1, TELEGRAM_RETRIES + 1):
            try:
                async with session.post(url, json=payload) as r:
                    body = await r.text()
                    if r.status == 429:
                        # Telegram часто отдаёт retry_after в JSON
                        # {"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7"}
                        m = re.search(r"retry after (\d+)", body, flags=re.IGNORECASE)
                        wait_s = int(m.group(1)) if m else (2 + attempt)
                        await asyncio.sleep(wait_s)
                        raise RuntimeError(f"Telegram 429: {body}")
                    if r.status >= 400:
                        raise RuntimeError(f"Telegram error {r.status}: {body}")
                    return
            except Exception as e:
                last_err = e
                if attempt < TELEGRAM_RETRIES:
                    await asyncio.sleep(1.2 * attempt)
                else:
                    raise RuntimeError(str(last_err))


async def main():
    sources = load_sources()
    state = load_state()

    timeout = ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.8",
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        # собираем по регуляторам: один регулятор = одно сообщение
        regulator_to_urls: Dict[str, List[str]] = {}
        for s in sources:
            reg = str(s["regulator"]).strip()
            urls = [normalize_url(u) for u in s["urls"]]
            regulator_to_urls.setdefault(reg, [])
            regulator_to_urls[reg].extend(urls)

        for regulator, urls in regulator_to_urls.items():
            all_items: List[Item] = []
            errors: List[Tuple[str, str]] = []

            # по каждому источнику регулятора
            for u in urls:
                items, err = await collect_items_for_source(session, regulator, u)
                if err:
                    errors.append((u, err))
                else:
                    all_items.extend(items)

            # дедуп по url + отсеивание "мусора"
            uniq: Dict[str, Item] = {}
            for it in all_items:
                if is_drop_url(it.url):
                    continue
                if it.url not in uniq:
                    uniq[it.url] = it

            # считаем новое
            new_items = []
            for it in uniq.values():
                if is_new_and_mark(state, regulator, it.url):
                    new_items.append(it)

            # лимит на сообщение
            new_items = new_items[:MAX_ITEMS_PER_REGULATOR]

            # если есть новые — отправляем одним сообщением
            if new_items:
                msg = format_message(regulator, new_items)
                await send_tg(session, msg)

            # ошибки источников отправляем отдельно (но тоже одним сообщением на регулятора)
            # чтобы не спамить — отправляем только если нет новых публикаций (или если хочешь всегда — убери условие)
            if errors and not new_items:
                lines = []
                lines.append("Мониторинг НПА (ИБ)")
                lines.append(f"Регулятор: {regulator}")
                lines.append(f"Дата: {now_msk_str()}")
                lines.append("Ошибки источников:")
                lines.append("")
                for (u, e) in errors[:5]:
                    lines.append(f"- Источник: {u}")
                    lines.append(f"  Причина: {e}")
                await send_tg(session, "\n".join(lines))

    compact_seen(state, keep_days=45)
    save_json_file(STATE_FILE, state)


if __name__ == "__main__":
    asyncio.run(main())
