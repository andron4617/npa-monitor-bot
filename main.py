import asyncio
import html as html_lib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import ClientTimeout
from openai import AsyncOpenAI

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

SOURCES_FILE = "sources.json"
STATE_FILE = "state.json"

MAX_ITEMS_PER_REGULATOR = 5
HTTP_TIMEOUT_SECONDS = 25
HTTP_RETRIES = 3
HTTP_RETRY_BACKOFF = 1.6
TELEGRAM_MAX_CHARS = 3500
TELEGRAM_RETRIES = 5

OPENAI_MODEL = "gpt-5.4"
OPENAI_MAX_CONCURRENT = 2

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

DROP_URL_PATTERNS = [
    r"^tel:",
    r"^mailto:",
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
    r"/news(?!/)",
    r"/Localization/",
    r"/SwitchLanguage",
    r"/help",
    r"/OpenData",
    r"/HtmlConstructor",
    r"/calendar/",
    r"/search/",
    r"/documents/(daily|weekly|monthly)\b",
    r"/documents/block/[^?]+(\?index=\d+)?$",
    r"/_nuxt/",
]

DROP_EXTENSIONS = {
    ".js", ".css", ".map", ".png", ".jpg", ".jpeg", ".gif", ".svg",
    ".webp", ".ico", ".woff", ".woff2", ".ttf", ".eot", ".json", ".xml"
}

DOC_URL_PATTERNS = [
    r"https?://publication\.pravo\.gov\.ru/document/\d+",
    r"https?://cbr\.ru/Crosscut/LawActs/File/\d+",
    r"https?://cbr\.ru/Content/Document/File/\d+/.+\.pdf",
    r"https?://cbr\.ru/Content/Document/File/\d+/.+",
    r"https?://regulation\.gov\.ru/projects/\d+",
    r"https?://www\.rst\.gov\.ru/portal/gost/.*",
    r"https?://fstec\.ru/.*",
]

PRAVO_DOC_RE = re.compile(r"https?://publication\.pravo\.gov\.ru/document/\d+")
REGULATION_PROJECT_RE = re.compile(r"https?://regulation\.gov\.ru/projects/\d+")

TEXT_DROP_PATTERNS = [
    r"служебн\w*\s+поведени\w*",
    r"конфликт\w*\s+интерес\w*",
    r"доход\w*[, ]+расход\w*[, ]+имуществ\w*",
    r"обязательств\w*\s+имуществен\w*\s+характер\w*",
    r"социальн\w*\s+гаранти\w*",
    r"вступительн\w*\s+испытан\w*",
    r"конкурс\w*\s+на\s+замещен\w*",
    r"вакантн\w*\s+должност\w*",
    r"гражданск\w*\s+оборон\w*",
    r"состав\w*\s+коллеги\w*",
    r"официальн\w*\s+представител\w*",
    r"аккредитаци\w*\s+российск\w*\s+организац\w*",
    r"значим\w*\s+разработчик\w*",
    r"служб\w*\s+этик\w*",
]

TEXT_KEEP_PATTERNS = [
    r"защит\w*\s+информац\w*",
    r"информационн\w*\s+безопасност\w*",
    r"кибербезопасност\w*",
    r"критическ\w*\s+информационн\w*\s+инфраструктур\w*",
    r"\bкии\b",
    r"персональн\w*\s+данн\w*",
    r"криптограф\w*",
    r"шифрован\w*",
    r"\bскзи\b",
    r"\bори\b",
    r"организатор\w*\s+распространени\w*\s+информац\w*",
    r"реестр\w*\s+ори",
    r"надзор\w*\s+в\s+област\w*\s+связи",
    r"проверочн\w*\s+лист",
    r"оператор\w*\s+связи",
    r"защит\w*\s+персональн\w*\s+данн\w*",
]


@dataclass(frozen=True)
class Item:
    title: str
    url: str
    content_preview: str = ""


def now_msk_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def is_drop_url(url: str) -> bool:
    u = url.strip()

    for p in DROP_URL_PATTERNS:
        if re.search(p, u, flags=re.IGNORECASE):
            return True

    path = urlparse(u).path.lower()
    for ext in DROP_EXTENSIONS:
        if path.endswith(ext):
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
            async with session.get(url, allow_redirects=True) as r:
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


def extract_links_from_html(base_url: str, page_html: str) -> List[str]:
    links = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', page_html, flags=re.IGNORECASE):
        href = m.group(1).strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        links.append(abs_url)
    return links


def pick_document_links(urls: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()

    for u in urls:
        u = normalize_url(u)
        if not u or u in seen:
            continue
        seen.add(u)

        if is_drop_url(u):
            continue

        ok = False
        for p in DOC_URL_PATTERNS:
            if re.search(p, u, flags=re.IGNORECASE):
                ok = True
                break

        if "publication.pravo.gov.ru" in u:
            ok = bool(PRAVO_DOC_RE.search(u))

        if "regulation.gov.ru" in u:
            ok = bool(REGULATION_PROJECT_RE.search(u))

        if "www.nspk.ru" in u:
            path = urlparse(u).path.lower()
            ok = any(path.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx"])

        if ok:
            out.append(u)

    return out


def clean_title(raw_title: str) -> str:
    title = raw_title or ""
    title = html_lib.unescape(title)
    title = normalize_spaces(title)
    title = title.replace("∙", "·")
    title = title.replace("&nbsp;", " ")
    return title[:240] if title else "Документ"


def extract_text_preview_from_html(page_html: str) -> str:
    page_html = re.sub(r"(?is)<script.*?>.*?</script>", " ", page_html)
    page_html = re.sub(r"(?is)<style.*?>.*?</style>", " ", page_html)
    page_html = re.sub(r"(?is)<head.*?>.*?</head>", " ", page_html)
    page_html = re.sub(r"(?is)<[^>]+>", " ", page_html)
    page_html = html_lib.unescape(page_html)
    page_html = normalize_spaces(page_html)
    return page_html[:3500]


def combined_item_text(item: Item) -> str:
    return normalize_spaces(f"{item.title} {item.content_preview}".lower())


def text_matches_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)


def prefilter_item(item: Item, regulator: str) -> bool:
    text = combined_item_text(item)

    if text_matches_any(text, TEXT_DROP_PATTERNS):
        return False

    if regulator in {"ФСТЭК", "ФСБ", "Минцифры", "Роскомнадзор", "Банк России", "Проекты НПА"}:
        return True

    return text_matches_any(text, TEXT_KEEP_PATTERNS)


async def title_and_preview_for_doc(session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
    try:
        page_html = await fetch_text(session, url)

        title = "Документ"
        m = re.search(r"<title>(.*?)</title>", page_html, flags=re.IGNORECASE | re.DOTALL)
        if m:
            title = clean_title(m.group(1))

        preview = extract_text_preview_from_html(page_html)
        return title, preview
    except Exception:
        return "Документ", ""


async def collect_items_for_source(
    session: aiohttp.ClientSession,
    source_name: str,
    url: str,
) -> Tuple[List[Item], Optional[str]]:
    try:
        page_html = await fetch_text(session, url)
        links = extract_links_from_html(url, page_html)
        doc_links = pick_document_links(links)
        doc_links = doc_links[:MAX_ITEMS_PER_REGULATOR * 6]

        items: List[Item] = []
        for link in doc_links:
            title = "Документ"
            preview = ""

            if "publication.pravo.gov.ru/document/" in link:
                title, preview = await title_and_preview_for_doc(session, link)
            elif "regulation.gov.ru/projects/" in link:
                title = "Проект НПА"
            elif "cbr.ru" in link:
                title = "Документ Банка России"
            elif "rst.gov.ru" in link:
                title = "Документ Росстандарта"
            elif "fstec.ru" in link:
                title = "Документ ФСТЭК"

            items.append(Item(title=title, url=link, content_preview=preview))

        uniq: Dict[str, Item] = {}
        for it in items:
            if it.url not in uniq:
                uniq[it.url] = it

        return list(uniq.values()), None

    except asyncio.TimeoutError:
        return [], "Источник не ответил вовремя"
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


async def analyze_item_with_openai(
    client: AsyncOpenAI,
    regulator: str,
    item: Item,
    sem: asyncio.Semaphore,
) -> Dict[str, str]:
    fallback = {
        "relevance": "ошибка AI",
        "summary": "AI-анализ не выполнен",
        "title": item.title,
        "ai_debug": "неизвестная ошибка"
    }

    if not OPENAI_API_KEY:
        fallback["ai_debug"] = "OPENAI_API_KEY не задан"
        return fallback

    preview = item.content_preview[:2200] if item.content_preview else ""

    prompt = f"""
Ты анализируешь нормативную публикацию для Telegram-бота мониторинга НПА.

ТВОЯ ЗАДАЧА:
1. Определить РЕЛЕВАНТНОСТЬ ИМЕННО ДЛЯ ИНФОРМАЦИОННОЙ БЕЗОПАСНОСТИ.
2. Дать краткое описание только по доступным данным, без догадок.

СЧИТАЙ ВЫСОКОЙ ИБ-РЕЛЕВАНТНОСТЬЮ только документы, которые прямо относятся к:
- защите информации
- информационной безопасности
- кибербезопасности
- криптографии / СКЗИ
- КИИ
- персональным данным и их защите
- требованиям к ОРИ, операторам связи, цифровым платформам, если это связано с безопасностью, хранением, доступом, контролем
- обязательным техническим, организационным или контрольным мерам в ИТ/ИБ

СЧИТАЙ НИЗКОЙ ИБ-РЕЛЕВАНТНОСТЬЮ документы про:
- кадры
- служебное поведение
- конфликт интересов
- доходы/расходы/имущество
- соцгарантии
- конкурсы
- вступительные испытания
- внутренние оргвопросы
- гражданскую оборону без прямой ИБ-составляющей

СЧИТАЙ "НЕЯСНО", если из текста и заголовка нельзя уверенно сделать вывод.

НЕ ДОДУМЫВАЙ:
- не придумывай содержание, если его нет
- не ставь высокую релевантность только из-за названия ведомства

ИСХОДНЫЕ ДАННЫЕ:
Регулятор: {regulator}
Заголовок: {item.title}
Ссылка: {item.url}
Текст:
{preview if preview else "Нет доступного текста, есть только заголовок и ссылка."}

Верни СТРОГО JSON без markdown:

{{
  "relevance": "высокая|средняя|низкая|неясно",
  "summary": "1-2 предложения только по фактам из доступного текста",
  "title": "короткий понятный заголовок без фантазий"
}}
""".strip()

    try:
        async with sem:
            response = await client.responses.create(
                model=OPENAI_MODEL,
                input=prompt
            )

        raw = (response.output_text or "").strip()
        if not raw:
            fallback["ai_debug"] = "пустой ответ модели"
            return fallback

        match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not match:
            fallback["ai_debug"] = f"ответ не похож на JSON: {raw[:300]}"
            return fallback

        try:
            data = json.loads(match.group(0))
        except Exception as e:
            fallback["ai_debug"] = f"JSON parse error: {type(e).__name__}: {e}; raw={raw[:300]}"
            return fallback

        title = clean_title(str(data.get("title", item.title)).strip()) or item.title
        relevance = str(data.get("relevance", "неясно")).strip().lower()
        summary = normalize_spaces(str(data.get("summary", "Описание не получено")).strip())

        if relevance not in {"высокая", "средняя", "низкая", "неясно"}:
            relevance = "неясно"

        return {
            "relevance": relevance,
            "summary": summary,
            "title": title,
            "ai_debug": "ok"
        }

    except Exception as e:
        fallback["ai_debug"] = f"{type(e).__name__}: {e}"
        return fallback


def should_send_after_ai(item: Dict[str, str], regulator: str) -> bool:
    relevance = item.get("relevance", "неясно").lower()

    if relevance == "низкая":
        return False

    if relevance == "неясно":
        return regulator in {"ФСТЭК", "ФСБ", "Минцифры", "Роскомнадзор", "Банк России", "Проекты НПА"}

    return True


def format_message(regulator: str, analyzed_items: List[Dict[str, str]]) -> str:
    lines = []
    lines.append("Мониторинг НПА")
    lines.append(f"Регулятор: {regulator}")
    lines.append(f"Дата: {now_msk_str()}")
    lines.append(f"Новые публикации: {len(analyzed_items)}")
    lines.append("")

    for i, it in enumerate(analyzed_items, 1):
        lines.append(f"{i}) {it['title']}")
        lines.append(f"Релевантность ИБ: {it['relevance']}")
        lines.append(f"Кратко: {it['summary']}")
        lines.append(f"Ссылка: {it['url']}")
        if it.get("ai_debug") and it["ai_debug"] != "ok":
            lines.append(f"AI_DEBUG: {it['ai_debug']}")
        lines.append("")

    return "\n".join(lines).strip()


async def send_tg(session: aiohttp.ClientSession, text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("BOT_TOKEN/CHAT_ID не заданы в env")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    chunks = []
    t = text.strip()
    while t:
        chunk = t[:TELEGRAM_MAX_CHARS]
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

    openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
    ai_sem = asyncio.Semaphore(OPENAI_MAX_CONCURRENT)

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        regulator_to_urls: Dict[str, List[str]] = {}
        for s in sources:
            reg = str(s["regulator"]).strip()
            urls = [normalize_url(u) for u in s["urls"]]
            regulator_to_urls.setdefault(reg, [])
            regulator_to_urls[reg].extend(urls)

        for regulator, urls in regulator_to_urls.items():
            all_items: List[Item] = []
            errors: List[Tuple[str, str]] = []

            for u in urls:
                items, err = await collect_items_for_source(session, regulator, u)
                if err:
                    errors.append((u, err))
                else:
                    all_items.extend(items)

            uniq: Dict[str, Item] = {}
            for it in all_items:
                if is_drop_url(it.url):
                    continue
                if it.url not in uniq:
                    uniq[it.url] = it

            new_items: List[Item] = []
            for it in uniq.values():
                if is_new_and_mark(state, regulator, it.url):
                    new_items.append(it)

            prefiltered_items = [it for it in new_items if prefilter_item(it, regulator)]
            prefiltered_items = prefiltered_items[:MAX_ITEMS_PER_REGULATOR]

            if prefiltered_items:
                analyzed_items: List[Dict[str, str]] = []

                if openai_client:
                    tasks = [
                        analyze_item_with_openai(openai_client, regulator, item, ai_sem)
                        for item in prefiltered_items
                    ]
                    ai_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for item, ai_result in zip(prefiltered_items, ai_results):
                        if isinstance(ai_result, Exception):
                            analyzed_items.append({
                                "title": item.title,
                                "relevance": "ошибка AI",
                                "summary": "Исключение верхнего уровня в AI-анализе",
                                "url": item.url,
                                "ai_debug": f"{type(ai_result).__name__}: {ai_result}",
                            })
                        else:
                            analyzed_items.append({
                                "title": ai_result.get("title", item.title),
                                "relevance": ai_result.get("relevance", "ошибка AI"),
                                "summary": ai_result.get("summary", "AI-анализ не выполнен"),
                                "url": item.url,
                                "ai_debug": ai_result.get("ai_debug", "no-debug"),
                            })
                else:
                    for item in prefiltered_items:
                        analyzed_items.append({
                            "title": item.title,
                            "relevance": "ошибка AI",
                            "summary": "OpenAI API не подключен",
                            "url": item.url,
                            "ai_debug": "OPENAI_API_KEY не найден",
                        })

                final_items = [it for it in analyzed_items if should_send_after_ai(it, regulator)]

                if final_items:
                    msg = format_message(regulator, final_items)
                    await send_tg(session, msg)

            if errors and not prefiltered_items:
                lines = []
                lines.append("Мониторинг НПА")
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
