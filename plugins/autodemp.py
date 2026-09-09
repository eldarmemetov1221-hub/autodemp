# -*- coding: utf-8 -*-
"""
AutoDemp — плагин автодемпинга цен для FunPayCardinal (FPC).

Плагин следит за конкурентами в подкатегории лота и автоматически ставит цену
на `шаг` ниже минимальной подходящей цены конкурента, соблюдая нижнюю границу
(MIN PRICE) и защиту от ценовой войны (лимит изменений цены в минуту).

Совместим с архитектурой FunPayCardinal (sidor0912/FunPayCardinal):
    * cardinal.account            -> FunPayAPI.Account
    * account.get_lot_fields()    -> получение полей и текущей цены лота
    * account.save_lot()          -> сохранение (изменение) цены лота
    * account.get_subcategory_public_lots() -> список публичных лотов (конкуренты)
    * cardinal.telegram (TGBot)   -> интерфейс настроек в Telegram
    * CBT.PLUGIN_SETTINGS         -> кнопка "Настройки" на карточке плагина

Устанавливается копированием файла в папку `plugins/` вашего FunPayCardinal.
"""

from __future__ import annotations

import os
import re
import json
import time
import logging
import threading
from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cardinal import Cardinal
    from telebot.types import CallbackQuery, Message

# --------------------------------------------------------------------------- #
#                       Метаданные плагина для FunPayCardinal                  #
# --------------------------------------------------------------------------- #
NAME = "AutoDemp"
VERSION = "1.0.0"
DESCRIPTION = (
    "Автодемпинг цен: держит цену на шаг ниже конкурентов, соблюдая нижнюю "
    "границу (MIN PRICE) и лимит изменений цены в минуту. Игнор-список "
    "продавцов, исключение своих лотов, подробные логи и настройка из Telegram."
)
CREDITS = "@autodemp"
UUID = "b0f6e9a2-9d7e-4c8a-8b1f-3a2d7c4e5f10"
SETTINGS_PAGE = True

logger = logging.getLogger("FPC.autodemp")
LOGGER_PREFIX = "[AutoDemp]"

# --------------------------------------------------------------------------- #
#                              Пути и конфигурация                             #
# --------------------------------------------------------------------------- #
# Каталог storage существует в любой установке FunPayCardinal.
CONFIG_DIR = os.path.join("storage", "plugins")
CONFIG_PATH = os.path.join(CONFIG_DIR, "autodemp.json")

# Фильтр конкурентов по способу доставки (определяется по тексту лота).
DELIVERY_MODES = ("all", "id", "code")
DELIVERY_TITLES = {"all": "все", "id": "по ID", "code": "по коду"}

# Значения по умолчанию для нового лота.
DEFAULT_LOT = {
    "enabled": False,          # запущен ли демпинг по этому лоту
    "min_price": 0.0,          # нижняя граница (никогда не опускаемся ниже)
    "max_price": 0.0,          # верхняя граница диапазона учёта конкурентов
    "step": 0.01,              # на сколько опускаемся ниже конкурента
    "interval": 5.0,           # период проверки, сек (поддерживает дробные)
    "no_competitor_strategy": "keep",  # keep | max | custom
    "custom_price": 0.0,       # цена для стратегии "custom"
    # --- фильтры конкурентов ---
    "delivery": "all",         # all | id | code (способ выдачи)
    "online_only": False,      # учитывать только онлайн-продавцов
    "min_reviews": 0,          # минимум отзывов у конкурента (0 = без ограничения)
    "keyword": "",             # текст, который должен быть в описании конкурента
    # Агрессивный режим: при наличии конкурентов сразу вставать на MIN
    # (гарантированно первый, минуя пошаговую гонку и лаг FunPay).
    "aggressive": False,
}

# Стратегии, когда подходящих конкурентов нет (по умолчанию — не менять цену).
NO_COMP_STRATEGIES = ("keep", "max", "custom")
NO_COMP_TITLES = {
    "keep": "не менять",
    "max": "макс. цена",
    "custom": "заданная цена",
}

# Технические ограничения FunPay: слишком частые запросы приводят к блокировке.
# Плагин не опускается ниже безопасного интервала, даже если пользователь
# запросил меньше (п.9 ТЗ — использовать макс. допустимый безопасный интервал).
MIN_SAFE_INTERVAL = 2.0        # сек — минимально безопасный период проверки
# Короткий кэш списка конкурентов: дедуп одинаковых запросов и экономия
# обращений, когда несколько лотов относятся к одной подкатегории.
COMP_CACHE_TTL = 1.5           # сек

# Синхронизация с обновлением таблицы FunPay (экспериментально): плагин ловит
# момент обновления таблицы и меняет цену через sync_delay секунд ПОСЛЕ него,
# чтобы свежая цена попала в ближайший снимок и вы оказались первым.
SYNC_POLL = 2.0                # частый опрос для отслеживания обновлений, сек

# Режим цены, в котором пользователь задаёт MIN/MAX/шаг и видит числа:
#   "seller" — цена для продавца (как в таблице FunPay, по умолчанию);
#   "buyer"  — цена для покупателя (с комиссией FunPay). Конвертация точная,
#              по коэффициенту комиссии из FunPay (CalcResult).
PRICE_MODES = ("seller", "buyer")
PRICE_MODE_TITLES = {"seller": "для продавца", "buyer": "для покупателя"}

# Значения по умолчанию для всего плагина.
DEFAULT_CONFIG = {
    "max_changes_per_min": 10,   # защита от ценовой войны
    "ignored_sellers": [],       # id продавцов, чьи лоты игнорируются
    "price_mode": "seller",      # seller | buyer
    # Экспериментально: обход серверного кэша FunPay (cache-buster) для более
    # свежих цен конкурентов. Помогает НЕ во всех разделах и повышает нагрузку.
    "fast_check": False,
    # Экспериментально: синхронизация с обновлением таблицы FunPay —
    # менять цену через sync_delay секунд ПОСЛЕ обновления таблицы, чтобы свежая
    # цена попала в ближайший снимок (цикл нестатичный, ≥30с — привязка к событию).
    "sync_refresh": False,
    "sync_delay": 7.0,           # сек после обновления таблицы до смены цены
    "lots": {},                  # {lot_id(str): {...}}
}

# Кэш коэффициента комиссии по подкатегории (комиссия стабильна, обновляем редко).
_COMM_CACHE: dict[tuple, tuple[float, float]] = {}   # key -> (timestamp, coeff)
_COMM_CACHE_LOCK = threading.Lock()
COMM_CACHE_TTL = 600.0        # сек — как часто перепроверять комиссию

# Трекер обновлений таблицы по подкатегории (для sync_refresh):
# {subcat_id: {"fp": последний отпечаток, "last": время посл. обновления,
#              "gaps": последние периоды между обновлениями}}
_REFRESH: dict[Any, dict] = {}
_REFRESH_LOCK = threading.Lock()

# Кэш полей лота (своя цена/подкатегория/валюта), чтобы не запрашивать поля лота
# каждый цикл. Поля перечитываются при реальном изменении цены и раз в LOT_META_TTL
# секунд (сверка на случай ручного изменения). Экономит ~половину запросов.
_LOT_META: dict[str, dict] = {}
_META_LOCK = threading.Lock()
LOT_META_TTL = 60.0            # сек — как часто сверять поля лота без изменения цены

# --------------------------------------------------------------------------- #
#                       Глобальное состояние времени выполнения               #
# --------------------------------------------------------------------------- #
_CFG: dict[str, Any] = json.loads(json.dumps(DEFAULT_CONFIG))
_CFG_LOCK = threading.RLock()          # защищает _CFG и реестры воркеров

_WORKERS: dict[str, dict[str, Any]] = {}   # lot_id -> {"thread", "stop"}
_LOT_LOCKS: dict[str, threading.Lock] = {}  # lot_id -> Lock (защита изменения лота)

# Живая статистика для интерфейса (не сохраняется на диск).
_STATS: dict[str, dict[str, Any]] = {}     # lot_id -> {...}

# Глобальный лимитер изменений цены (защита от ценовой войны / зацикливания).
_RATE_TIMES: deque = deque()
_RATE_LOCK = threading.Lock()

# Кэш конкурентов по подкатегории + per-key блокировка (дедуп одинаковых
# одновременных запросов, п.9 ТЗ). {key: (timestamp, lots)}
_COMP_CACHE: dict[tuple, tuple[float, list]] = {}
_COMP_CACHE_LOCK = threading.Lock()
_COMP_FETCH_LOCKS: dict[tuple, threading.Lock] = {}
# Сериализует временный monkey-patch account.method при cache-buster запросе.
_METHOD_PATCH_LOCK = threading.Lock()
# Пути публичного списка лотов, к которым добавляем cache-buster.
_PUBLIC_LOTS_RE = re.compile(r"^(lots|chips)/\d+/$")

_CARDINAL: "Cardinal | None" = None

# --------------------------------------------------------------------------- #
#                     Совместимость с версиями FunPayAPI                       #
# --------------------------------------------------------------------------- #
try:  # requests есть всегда, но импортируем аккуратно.
    import requests
    _NETWORK_ERRORS: tuple = (requests.exceptions.RequestException,)
except Exception:  # pragma: no cover
    _NETWORK_ERRORS = ()

try:
    from FunPayAPI.common.exceptions import RequestFailedError  # type: ignore
    _FUNPAY_ERRORS: tuple = (RequestFailedError,)
except Exception:  # pragma: no cover — старые/иные версии
    _FUNPAY_ERRORS = ()

_RETRYABLE_ERRORS = _NETWORK_ERRORS + _FUNPAY_ERRORS


# --------------------------------------------------------------------------- #
#                                 Утилиты                                      #
# --------------------------------------------------------------------------- #
def _now_str() -> str:
    return time.strftime("%H:%M:%S")


def _currency_symbol(currency: Any) -> str:
    """Пытается получить символ валюты лота, безопасно для любой версии API."""
    try:
        name = getattr(currency, "name", "") or ""
    except Exception:
        name = ""
    return {"RUB": "₽", "USD": "$", "EUR": "€"}.get(name.upper(), "₽")


def _fmt(price: float | None, symbol: str = "₽") -> str:
    if price is None:
        return "—"
    return f"{price:.2f} {symbol}".rstrip()


def _prices_equal(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) < 0.005


def load_config() -> None:
    """Загружает конфигурацию с диска (вызывается при старте)."""
    global _CFG
    with _CFG_LOCK:
        data = json.loads(json.dumps(DEFAULT_CONFIG))
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    data.update({k: v for k, v in loaded.items() if k in DEFAULT_CONFIG})
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Не удалось прочитать конфиг: {e}")
        # Нормализация лотов.
        lots = {}
        for lot_id, lot in (data.get("lots") or {}).items():
            merged = dict(DEFAULT_LOT)
            if isinstance(lot, dict):
                merged.update({k: v for k, v in lot.items() if k in DEFAULT_LOT})
            if merged["no_competitor_strategy"] not in NO_COMP_STRATEGIES:
                merged["no_competitor_strategy"] = "keep"
            try:
                merged["interval"] = float(merged.get("interval", MIN_SAFE_INTERVAL))
            except (TypeError, ValueError):
                merged["interval"] = MIN_SAFE_INTERVAL
            if merged.get("delivery") not in DELIVERY_MODES:
                merged["delivery"] = "all"
            merged["online_only"] = bool(merged.get("online_only", False))
            try:
                merged["min_reviews"] = max(0, int(merged.get("min_reviews", 0) or 0))
            except (TypeError, ValueError):
                merged["min_reviews"] = 0
            merged["keyword"] = str(merged.get("keyword", "") or "")
            merged["aggressive"] = bool(merged.get("aggressive", False))
            lots[str(lot_id)] = merged
        data["lots"] = lots
        try:
            data["ignored_sellers"] = [int(x) for x in (data.get("ignored_sellers") or [])]
        except Exception:
            data["ignored_sellers"] = []
        try:
            data["max_changes_per_min"] = max(1, int(data.get("max_changes_per_min", 10)))
        except Exception:
            data["max_changes_per_min"] = 10
        if data.get("price_mode") not in PRICE_MODES:
            data["price_mode"] = "seller"
        data["fast_check"] = bool(data.get("fast_check", False))
        data["sync_refresh"] = bool(data.get("sync_refresh", False))
        try:
            data["sync_delay"] = max(0.0, float(data.get("sync_delay", 7.0)))
        except (TypeError, ValueError):
            data["sync_delay"] = 7.0
        _CFG = data


def save_config() -> None:
    """Атомарно сохраняет конфигурацию (переживает перезапуск Cardinal)."""
    with _CFG_LOCK:
        try:
            os.makedirs(CONFIG_DIR, exist_ok=True)
            tmp = CONFIG_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_CFG, f, ensure_ascii=False, indent=2)
            os.replace(tmp, CONFIG_PATH)
        except Exception as e:
            logger.error(f"{LOGGER_PREFIX} Не удалось сохранить конфиг: {e}")


def _get_lot_cfg(lot_id: str) -> dict[str, Any] | None:
    with _CFG_LOCK:
        lot = _CFG["lots"].get(str(lot_id))
        return dict(lot) if lot else None


def _lot_lock(lot_id: str) -> threading.Lock:
    with _CFG_LOCK:
        lock = _LOT_LOCKS.get(str(lot_id))
        if lock is None:
            lock = threading.Lock()
            _LOT_LOCKS[str(lot_id)] = lock
        return lock


def _set_stat(lot_id: str, **kwargs) -> None:
    with _CFG_LOCK:
        st = _STATS.setdefault(str(lot_id), {})
        st.update(kwargs)


def _get_stat(lot_id: str) -> dict[str, Any]:
    with _CFG_LOCK:
        return dict(_STATS.get(str(lot_id), {}))


# --------------------------------------------------------------------------- #
#                       Защита от ценовой войны (rate limit)                   #
# --------------------------------------------------------------------------- #
def _rate_allow() -> bool:
    """True, если сейчас разрешено изменять цену (лимит в минуту не превышен)."""
    with _CFG_LOCK:
        limit = int(_CFG.get("max_changes_per_min", 10))
    now = time.time()
    with _RATE_LOCK:
        while _RATE_TIMES and now - _RATE_TIMES[0] > 60:
            _RATE_TIMES.popleft()
        return len(_RATE_TIMES) < max(1, limit)


def _rate_register() -> None:
    with _RATE_LOCK:
        _RATE_TIMES.append(time.time())


def _refresh_track_and_gate(subcat_id, competitors, delay: float) -> bool:
    """
    Отслеживает обновления таблицы по смене «отпечатка» цен конкурентов.
    Возвращает True, если с момента последнего обновления прошло >= delay секунд
    — то есть пора менять цену «через N секунд ПОСЛЕ обновления» (идея пользователя).
    Цикл обновления у FunPay нестатичный, поэтому привязываемся к самому событию
    обновления, а не пытаемся предсказать следующее.
    """
    fp = tuple(sorted(round(c.price, 2) for c in competitors if c.price is not None))
    now = time.time()
    with _REFRESH_LOCK:
        tr = _REFRESH.setdefault(subcat_id, {"fp": None, "last_refresh": None})
        if tr["fp"] is not None and fp != tr["fp"]:
            tr["last_refresh"] = now         # таблица только что обновилась
        elif tr["last_refresh"] is None:
            tr["last_refresh"] = now         # инициализация
        tr["fp"] = fp
        last = tr["last_refresh"]
    return (now - last) >= delay


def _safe_interval(interval: Any) -> float:
    """Ограничивает интервал снизу безопасным минимумом (п.9 ТЗ)."""
    try:
        value = float(interval)
    except (TypeError, ValueError):
        value = MIN_SAFE_INTERVAL
    return max(MIN_SAFE_INTERVAL, value)


def _fetch_lock(key: tuple) -> threading.Lock:
    with _COMP_CACHE_LOCK:
        lock = _COMP_FETCH_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _COMP_FETCH_LOCKS[key] = lock
        return lock


def _coeff_from_calc(calc_result) -> float | None:
    """Достаёт коэффициент комиссии (цена_покупателя / цена_продавца)."""
    if calc_result is None:
        return None
    k = getattr(calc_result, "commission_coefficient", None)
    try:
        k = float(k)
    except (TypeError, ValueError):
        return None
    return k if k > 0 else None


def _commission_coefficient(account, lf, subcat, stop: "threading.Event",
                            price_hint: float | None = None) -> float | None:
    """
    Коэффициент комиссии FunPay для подкатегории (buyer = seller * k).
    Источники по приоритету: поля лота -> account.calc() -> кэш.
    `lf` может быть None (тогда используется account.calc с price_hint).
    Возвращает None, если получить не удалось (тогда buyer-режим пропускает цикл,
    чтобы не выставить неверную цену).
    """
    key = (subcat.type, subcat.id)
    # 1. Из полей лота (без доп. запроса).
    k = _coeff_from_calc(getattr(lf, "calc_result", None))
    # 2. Через account.calc(), если в полях нет и кэш устарел.
    if k is None:
        now = time.time()
        with _COMM_CACHE_LOCK:
            cached = _COMM_CACHE.get(key)
        if cached and now - cached[0] <= COMM_CACHE_TTL:
            return cached[1]
        try:
            base = (getattr(lf, "price", None) or price_hint or 1000)
            calc = _call_with_retry(account.calc, subcat.type, subcat.id,
                                    stop=stop, price=base)
            k = _coeff_from_calc(calc)
        except Exception as e:
            logger.warning(f"{LOGGER_PREFIX} Не удалось рассчитать комиссию: {e}")
            k = None
    if k is not None:
        with _COMM_CACHE_LOCK:
            _COMM_CACHE[key] = (time.time(), k)
        return k
    # 3. Последнее известное значение из кэша (лучше, чем ничего).
    with _COMM_CACHE_LOCK:
        cached = _COMM_CACHE.get(key)
    return cached[1] if cached else None


def _get_competitors(account, subcat, stop: "threading.Event"):
    """
    Возвращает список публичных лотов подкатегории с коротким кэшированием.
    Дедуплицирует одинаковые одновременные запросы: пока один поток тянет
    данные, остальные ждут и берут результат из кэша (п.9 ТЗ).
    """
    key = (subcat.type, subcat.id)
    now = time.time()
    with _COMP_CACHE_LOCK:
        entry = _COMP_CACHE.get(key)
        if entry and now - entry[0] <= COMP_CACHE_TTL:
            return entry[1]

    lock = _fetch_lock(key)
    with lock:
        # Повторная проверка: пока ждали лок, другой поток мог обновить кэш.
        now = time.time()
        with _COMP_CACHE_LOCK:
            entry = _COMP_CACHE.get(key)
            if entry and now - entry[0] <= COMP_CACHE_TTL:
                return entry[1]
        lots = _fetch_public_lots(account, subcat, stop)
        with _COMP_CACHE_LOCK:
            _COMP_CACHE[key] = (time.time(), lots)
        return lots


def _fetch_public_lots(account, subcat, stop: "threading.Event"):
    """
    Запрашивает публичные лоты подкатегории. В режиме fast_check добавляет к
    URL cache-buster, чтобы обойти серверный кэш FunPay и получить более свежие
    цены (экспериментально). Разбор HTML — «родным» парсером FunPayAPI.
    """
    with _CFG_LOCK:
        fast = bool(_CFG.get("fast_check", False))
    if not fast:
        return _call_with_retry(
            account.get_subcategory_public_lots, subcat.type, subcat.id, stop=stop
        )

    # Временный monkey-patch account.method: добавляем cache-buster только к
    # пути публичного списка лотов (lots/{id}/ или chips/{id}/), не трогая
    # остальные запросы (save_lot и пр.). Сериализовано глобальным локом.
    with _METHOD_PATCH_LOCK:
        orig_method = account.method

        def _patched(request_method, api_method, *args, **kwargs):
            if isinstance(api_method, str) and _PUBLIC_LOTS_RE.match(api_method):
                sep = "&" if "?" in api_method else "?"
                api_method = f"{api_method}{sep}_cb={int(time.time() * 1000)}"
            return orig_method(request_method, api_method, *args, **kwargs)

        account.method = _patched
        try:
            return _call_with_retry(
                account.get_subcategory_public_lots, subcat.type, subcat.id, stop=stop
            )
        finally:
            account.method = orig_method


# --------------------------------------------------------------------------- #
#                          Работа с FunPay (с ретраями)                        #
# --------------------------------------------------------------------------- #
def _call_with_retry(func, *args, stop: "threading.Event | None" = None,
                     retries: int = 3, base_delay: float = 2.0, **kwargs):
    """
    Вызывает сетевую функцию FunPay с повтором после временной ошибки.
    Повышает исключение, если все попытки исчерпаны.
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        if stop is not None and stop.is_set():
            raise RuntimeError("stopped")
        try:
            return func(*args, **kwargs)
        except _RETRYABLE_ERRORS as e:  # временные сетевые/FunPay ошибки
            last_exc = e
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                f"{LOGGER_PREFIX} Временная ошибка ({func.__name__}): {e}. "
                f"Повтор через {delay:.0f} сек. (попытка {attempt}/{retries})"
            )
            if stop is not None:
                if stop.wait(delay):
                    raise RuntimeError("stopped")
            else:
                time.sleep(delay)
    if last_exc:
        raise last_exc
    raise RuntimeError("retry failed")


# --------------------------------------------------------------------------- #
#                         Основной алгоритм по одному лоту                     #
# --------------------------------------------------------------------------- #
def _process_lot_once(cardinal: "Cardinal", lot_id: str,
                      stop: "threading.Event") -> None:
    """Один цикл: посчитать конкурентов и при необходимости изменить цену."""
    account = cardinal.account
    lot_cfg = _get_lot_cfg(lot_id)
    if not lot_cfg:
        return

    min_price = float(lot_cfg["min_price"])
    max_price = float(lot_cfg["max_price"])
    step = float(lot_cfg["step"])
    strategy = lot_cfg["no_competitor_strategy"]
    custom_price = float(lot_cfg.get("custom_price", 0.0))
    f_delivery = lot_cfg.get("delivery", "all")
    f_online = bool(lot_cfg.get("online_only", False))
    f_min_reviews = int(lot_cfg.get("min_reviews", 0) or 0)
    f_keyword = (lot_cfg.get("keyword", "") or "").strip().lower()
    aggressive = bool(lot_cfg.get("aggressive", False))

    # Блокировка исключает одновременное изменение одного и того же лота.
    lock = _lot_lock(lot_id)
    if not lock.acquire(blocking=False):
        # Другой цикл уже работает с этим лотом — пропускаем.
        return
    try:
        # 1. Поля лота (своя цена/подкатегория/валюта) — берём из кэша, чтобы не
        #    запрашивать каждый цикл. Полный запрос: если кэша нет/устарел.
        now0 = time.time()
        with _META_LOCK:
            meta = _LOT_META.get(lot_id)
        need_full = (meta is None) or (now0 - meta.get("ts", 0) > LOT_META_TTL)
        lf = None
        if need_full:
            lf = _call_with_retry(account.get_lot_fields, int(lot_id), stop=stop)
            subcat = getattr(lf, "subcategory", None)
            if subcat is None:
                logger.error(f"{LOGGER_PREFIX} Лот {lot_id}: не удалось определить подкатегорию.")
                _set_stat(lot_id, error="нет подкатегории", updated=time.time())
                return
            symbol = _currency_symbol(getattr(lf, "currency", None))
            current_seller = lf.price
            with _META_LOCK:
                _LOT_META[lot_id] = {"subcat": subcat, "symbol": symbol,
                                     "price": current_seller, "ts": now0}
        else:
            subcat = meta["subcat"]
            symbol = meta["symbol"]
            current_seller = meta["price"]

        # Комиссия FunPay нужна ВСЕГДА: цена лота — для продавца, а цены
        # конкурентов из списка — уже для покупателя (с комиссией).
        with _CFG_LOCK:
            mode = _CFG.get("price_mode", "seller")
        k = _commission_coefficient(account, lf, subcat, stop, price_hint=current_seller)
        if not k:
            logger.error(
                f"{LOGGER_PREFIX} Лот {lot_id}: не удалось получить комиссию FunPay "
                f"— цикл пропущен."
            )
            _set_stat(lot_id, error="нет данных о комиссии", updated=time.time())
            return

        current_buyer = current_seller * k

        # Границы и шаг пользователя приводим к цене ПОКУПАТЕЛЯ.
        if mode == "buyer":
            min_b, max_b, step_b, custom_b = min_price, max_price, step, custom_price
        else:  # заданы цены продавца — переводим в покупательские
            min_b, max_b, step_b, custom_b = (min_price * k, max_price * k,
                                              step * k, custom_price * k)

        # 2. Конкуренты в подкатегории (с кэшем и дедупликацией запросов).
        competitors = _get_competitors(account, subcat, stop)

        # 3. Исключаем свои лоты и игнорируемых продавцов.
        my_id = getattr(account, "id", None)
        with _CFG_LOCK:
            ignored = set(_CFG.get("ignored_sellers", []))

        filtered = []
        for c in competitors:
            seller = getattr(c, "seller", None)
            seller_id = getattr(seller, "id", None) if seller else None
            if my_id is not None and seller_id == my_id:
                continue                      # мой собственный лот
            if seller_id in ignored:
                continue                      # игнорируемый продавец
            amount = getattr(c, "amount", None)
            if amount is not None and amount == 0:
                continue                      # лот недоступен (нет в наличии)

            # --- пользовательские фильтры конкурентов ---
            desc = (getattr(c, "description", None)
                    or getattr(c, "title", None) or "").lower()
            if f_delivery == "id" and "id" not in desc:
                continue                      # не «пополнение по ID»
            if f_delivery == "code" and "код" not in desc:
                continue                      # не «пополнение кодом»
            if f_keyword and f_keyword not in desc:
                continue                      # не подходит по ключевому слову
            if f_online and not getattr(seller, "online", False):
                continue                      # только онлайн-продавцы
            if f_min_reviews > 0 and int(getattr(seller, "reviews", 0) or 0) < f_min_reviews:
                continue                      # мало отзывов у продавца

            filtered.append(c)

        # Синхронизация с обновлением таблицы (экспериментально): менять цену
        # через sync_delay секунд ПОСЛЕ того, как таблица обновилась.
        with _CFG_LOCK:
            sync_on = bool(_CFG.get("sync_refresh", False))
            sync_delay = float(_CFG.get("sync_delay", 7.0))
        # Ключ трекера — сам лот (а не подкатегория): у лотов одной подкатегории
        # разные наборы конкурентов, и общий ключ приводил бы к взаимному сбросу.
        apply_ok = (_refresh_track_and_gate(lot_id, filtered, sync_delay)
                    if sync_on else True)

        # 4. Подходят по диапазону — в цене ПОКУПАТЕЛЯ (c.price уже покупательская).
        valid = [c for c in filtered
                 if c.price is not None and min_b <= c.price <= max_b]

        total_found = len(filtered)
        valid_count = len(valid)
        min_comp_buyer = min((c.price for c in valid), default=None)

        # 5. Целевая цена покупателя (без раннего округления — округляем в
        #    цене продавца при сохранении).
        target_b = None
        if valid:
            if aggressive:
                # Гарантированно первый: сразу на нижнюю границу (MIN).
                target_b = min_b
            else:
                target_b = min_comp_buyer - step_b
        elif strategy == "max":
            target_b = max_b if max_b > 0 else None
        elif strategy == "custom":
            target_b = custom_b if custom_b > 0 else None
        # keep (по умолчанию) — target_b остаётся None (не менять цену).
        if target_b is not None:
            if min_b > 0:
                target_b = max(target_b, min_b)     # защита: не ниже MIN PRICE
            if max_b > 0:
                target_b = min(target_b, max_b)

        # Значения для отображения — в единицах пользователя (buyer или seller).
        def _disp(buyer_val):
            if buyer_val is None:
                return None
            return buyer_val if mode == "buyer" else buyer_val / k

        current_price = _disp(current_buyer)
        min_comp = _disp(min_comp_buyer)
        target = _disp(target_b)

        _set_stat(
            lot_id,
            current_price=current_price, competitors=total_found,
            valid=valid_count, min_comp=min_comp, target=target,
            symbol=symbol, mode=mode, error=None, updated=time.time(),
            subcat_id=getattr(subcat, "id", None),
            subcat_name=getattr(subcat, "name", None),
        )

        mode_note = " (цены для покупателя)" if mode == "buyer" else ""
        header = (
            f"Lot {lot_id}{mode_note}\n"
            f"Найдены конкуренты: {total_found}\n"
            f"Подходят по диапазону: {valid_count}\n"
            f"Минимальная цена конкурента: {_fmt(min_comp, symbol)}\n"
            f"Текущая цена: {_fmt(current_price, symbol)}"
        )

        # 6. Меняем цену, если нужно. В FunPay сохраняется цена ПРОДАВЦА,
        #    поэтому целевую цену покупателя переводим в цену продавца (/ k).
        target_seller = round(target_b / k, 2) if target_b is not None else None
        cur_seller_r = round(current_seller, 2)

        # Гарантируем реальный подрез: FunPay сортирует по цене ПОКУПАТЕЛЯ,
        # поэтому моя цена покупателя должна быть строго ниже минимальной у
        # конкурента (с учётом округления цены продавца до 0.01).
        if target_seller is not None and valid and min_comp_buyer is not None:
            if target_seller * k >= min_comp_buyer:
                target_seller = round(target_seller - 0.01, 2)
            if min_b > 0:                           # но не ниже нижнего предела
                floor_seller = round(min_b / k, 2)
                if target_seller < floor_seller:
                    target_seller = floor_seller

        if target_seller is None or _prices_equal(target_seller, cur_seller_r):
            logger.info(f"{LOGGER_PREFIX} {header}\nЦена уже оптимальна: "
                        f"{_fmt(current_price, symbol)}")
            return

        # Синхронизация: изменение нужно, но ещё не прошла задержка после
        # обновления таблицы — ждём, чтобы новая цена попала в ближайший снимок.
        if not apply_ok:
            logger.info(f"{LOGGER_PREFIX} Лот {lot_id}: ждём {sync_delay:g} сек после "
                        f"обновления таблицы (синхронизация).")
            return

        # Нужно менять цену. Если поля лота ещё не загружены (работали по кэшу) —
        # грузим сейчас (нужны для сохранения) и сверяем реальную текущую цену.
        if lf is None:
            lf = _call_with_retry(account.get_lot_fields, int(lot_id), stop=stop)
            real_seller = lf.price
            with _META_LOCK:
                m = _LOT_META.get(lot_id)
                if m:
                    m["price"] = real_seller
                    m["ts"] = time.time()
            if _prices_equal(round(real_seller, 2), target_seller):
                # Кэш был устаревшим — реальная цена уже оптимальна.
                _set_stat(lot_id, current_price=_disp(real_seller * k), updated=time.time())
                logger.info(f"{LOGGER_PREFIX} {header}\nЦена уже оптимальна (сверено).")
                return
            current_seller = real_seller

        new_note = _fmt(target, symbol)
        if mode == "buyer":
            new_note += f" (покупатель) → {_fmt(target_seller, symbol)} продавцу"

        # Защита от ценовой войны — лимит изменений в минуту.
        if not _rate_allow():
            logger.warning(
                f"{LOGGER_PREFIX} {header}\nНовая цена: {new_note}\n"
                f"Пропуск: превышен лимит изменений цены в минуту "
                f"({_CFG.get('max_changes_per_min')})."
            )
            return

        try:
            lf.price = float(target_seller)
            # renew_fields внутри save_lot синхронизирует поля; дублируем явно.
            try:
                lf.fields["price"] = str(target_seller)
            except Exception:
                pass
            _call_with_retry(account.save_lot, lf, stop=stop)
            _rate_register()
            with _META_LOCK:                     # запоминаем новую свою цену
                m = _LOT_META.get(lot_id)
                if m:
                    m["price"] = float(target_seller)
                    m["ts"] = time.time()
            _set_stat(lot_id, current_price=_disp(target_seller * k), updated=time.time())
            logger.info(
                f"{LOGGER_PREFIX} {header}\nНовая цена: {new_note}\n"
                f"Цена изменена успешно"
            )
        except Exception as e:
            _set_stat(lot_id, error=str(e), updated=time.time())
            logger.error(
                f"{LOGGER_PREFIX} {header}\nОшибка изменения цены: {e}"
            )
    finally:
        lock.release()


# --------------------------------------------------------------------------- #
#                              Воркер (поток) лота                             #
# --------------------------------------------------------------------------- #
def _worker_loop(cardinal: "Cardinal", lot_id: str, stop: "threading.Event") -> None:
    logger.info(f"{LOGGER_PREFIX} Демпинг запущен для лота {lot_id}.")
    _set_stat(lot_id, status="running", error=None)
    while not stop.is_set():
        lot_cfg = _get_lot_cfg(lot_id)
        if not lot_cfg or not lot_cfg.get("enabled"):
            break

        # Уважаем ручное отключение плагина в интерфейсе Cardinal.
        if _plugin_disabled(cardinal):
            if stop.wait(5):
                break
            continue

        with _CFG_LOCK:
            sync_on = bool(_CFG.get("sync_refresh", False))
        # В режиме синхронизации опрашиваем часто (чтобы поймать окно обновления).
        interval = SYNC_POLL if sync_on else _safe_interval(
            lot_cfg.get("interval", MIN_SAFE_INTERVAL))
        try:
            _process_lot_once(cardinal, lot_id, stop)
        except RuntimeError as e:
            if str(e) == "stopped":
                break
            logger.error(f"{LOGGER_PREFIX} Лот {lot_id}: {e}")
            _set_stat(lot_id, error=str(e), updated=time.time())
        except Exception as e:
            # Полный перечень: сетевые ошибки, ошибки FunPay, прочее — цикл
            # не должен падать, ждём и продолжаем (восстановление после сбоев).
            logger.error(f"{LOGGER_PREFIX} Лот {lot_id}: непредвиденная ошибка: {e}")
            _set_stat(lot_id, error=str(e), updated=time.time())

        if stop.wait(interval):
            break

    _set_stat(lot_id, status="stopped")
    logger.info(f"{LOGGER_PREFIX} Демпинг остановлен для лота {lot_id}.")


def _plugin_disabled(cardinal: "Cardinal") -> bool:
    """True, если плагин выключен пользователем в меню плагинов Cardinal."""
    try:
        plugins = getattr(cardinal, "plugins", None)
        if not plugins:
            return False
        pl = plugins.get(UUID)
        if pl is None:
            return False
        return getattr(pl, "enabled", True) is False
    except Exception:
        return False


def start_lot(cardinal: "Cardinal", lot_id: str) -> None:
    """Запускает воркер демпинга для лота (idempotent)."""
    lot_id = str(lot_id)
    with _CFG_LOCK:
        w = _WORKERS.get(lot_id)
        if w and w["thread"].is_alive():
            return
        stop = threading.Event()
        thread = threading.Thread(
            target=_worker_loop, args=(cardinal, lot_id, stop),
            name=f"autodemp-{lot_id}", daemon=True,
        )
        _WORKERS[lot_id] = {"thread": thread, "stop": stop}
    thread.start()


def stop_lot(lot_id: str, join: bool = True) -> None:
    """Останавливает воркер демпинга для лота."""
    lot_id = str(lot_id)
    with _CFG_LOCK:
        w = _WORKERS.pop(lot_id, None)
    if not w:
        return
    w["stop"].set()
    if join:
        w["thread"].join(timeout=10)


def stop_all(join: bool = True) -> None:
    for lot_id in list(_WORKERS.keys()):
        stop_lot(lot_id, join=join)


# --------------------------------------------------------------------------- #
#                          Telegram: интерфейс настроек                        #
# --------------------------------------------------------------------------- #
# Префиксы callback_data (короткие, чтобы уложиться в лимит Telegram 64 байта).
CB_LOT = "ADlot"        # ADlot:<lot_id>
CB_ADD = "ADadd"        # ADadd
CB_SET = "ADset"        # ADset:<param>:<lot_id>
CB_STRAT = "ADstrat"    # ADstrat:<lot_id>
CB_TOGGLE = "ADtgl"     # ADtgl:<lot_id>
CB_DEL = "ADdel"        # ADdel:<lot_id>
CB_DEL_OK = "ADdok"     # ADdok:<lot_id>
CB_IGN = "ADign"        # ADign
CB_IGN_ADD = "ADigadd"  # ADigadd
CB_IGN_DEL = "ADigdel"  # ADigdel:<seller_id>
CB_RATE = "ADrate"      # ADrate
CB_MODE = "ADmode"      # ADmode — переключить режим цены (продавец/покупатель)
CB_FAST = "ADfast"      # ADfast — вкл/выкл экспериментальную «быструю проверку»
CB_SYNC = "ADsync"      # ADsync — вкл/выкл синхронизацию с обновлением таблицы
CB_SYNCDELAY = "ADsdl"  # ADsdl — задать задержку после обновления таблицы
CB_QUICK = "ADquick"    # ADquick — экран быстрого вкл/выкл лотов
CB_QTOGGLE = "ADqt"     # ADqt:<lot_id> — быстрый тумблер лота
CB_DELIV = "ADdlv"      # ADdlv:<lot_id> — цикл фильтра доставки
CB_ONLINE = "ADonl"     # ADonl:<lot_id> — вкл/выкл «только онлайн»
CB_AGGR = "ADaggr"      # ADaggr:<lot_id> — вкл/выкл агрессивный режим

# Состояния ввода (msg_handler по префиксу "AD:").
ST_ADD = "AD:add"
ST_SET = "AD:set"        # AD:set:<param>:<lot_id>
ST_IGN_ADD = "AD:igadd"
ST_RATE = "AD:rate"
ST_SYNCDELAY = "AD:syncdelay"


def _register_telegram(cardinal: "Cardinal") -> None:
    tg = getattr(cardinal, "telegram", None)
    if tg is None:
        logger.info(f"{LOGGER_PREFIX} Telegram отключён — интерфейс настроек недоступен.")
        return

    try:
        from tg_bot import CBT  # типы callback FunPayCardinal
    except Exception:
        class CBT:  # запасной вариант, если структура изменится
            PLUGIN_SETTINGS = "47"

    from telebot.types import InlineKeyboardMarkup as K, InlineKeyboardButton as B

    bot = tg.bot

    # ---------------------- построение экранов ---------------------- #
    def kb_main() -> "K":
        kb = K()
        with _CFG_LOCK:
            lots = dict(_CFG["lots"])
            rate = _CFG.get("max_changes_per_min", 10)
        for lot_id, lot in lots.items():
            mark = "🟢" if lot.get("enabled") else "🔴"
            st = _get_stat(lot_id)
            price = st.get("current_price")
            sym = st.get("symbol", "₽")
            label = f"{mark} {lot_id}"
            if price is not None:
                label += f" | {_fmt(price, sym)}"
            kb.add(B(label, callback_data=f"{CB_LOT}:{lot_id}"))
        with _CFG_LOCK:
            mode = _CFG.get("price_mode", "seller")
            fast = _CFG.get("fast_check", False)
            sync = _CFG.get("sync_refresh", False)
        if lots:
            kb.add(B("⚡ Вкл/выкл лоты", callback_data=CB_QUICK))
        kb.add(B("➕ Добавить лот", callback_data=CB_ADD))
        kb.add(B(f"💱 Цены: {PRICE_MODE_TITLES.get(mode)}", callback_data=CB_MODE))
        kb.add(B(f"⚡ Быстрая проверка (эксп.): {'ВКЛ' if fast else 'выкл'}",
                 callback_data=CB_FAST))
        kb.add(B(f"🕒 Синхр. с таблицей (эксп.): {'ВКЛ' if sync else 'выкл'}",
                 callback_data=CB_SYNC))
        if sync:
            with _CFG_LOCK:
                sdelay = _CFG.get("sync_delay", 7.0)
            kb.add(B(f"⏱ Задержка после обновления: {sdelay:g} сек",
                     callback_data=CB_SYNCDELAY))
        kb.add(B(f"🛡 Лимит изм./мин: {rate}", callback_data=CB_RATE))
        kb.add(B("🚫 Игнор-список продавцов", callback_data=CB_IGN))
        kb.add(B("◀️ Назад", callback_data=f"{CBT.EDIT_PLUGIN}:{UUID}:0"
                 if hasattr(CBT, "EDIT_PLUGIN") else f"45:{UUID}:0"))
        return kb

    def text_main() -> str:
        with _CFG_LOCK:
            lots = _CFG["lots"]
            rate = _CFG.get("max_changes_per_min", 10)
            ign = _CFG.get("ignored_sellers", [])
            mode = _CFG.get("price_mode", "seller")
            fast = _CFG.get("fast_check", False)
            sync = _CFG.get("sync_refresh", False)
        return (
            "🤖 <b>AutoDemp — автодемпинг</b>\n\n"
            f"Лотов настроено: <b>{len(lots)}</b>\n"
            f"Режим цены: <b>{PRICE_MODE_TITLES.get(mode)}</b>\n"
            f"Быстрая проверка: <b>{'ВКЛ' if fast else 'выкл'}</b>\n"
            f"Синхр. с таблицей: <b>{'ВКЛ' if sync else 'выкл'}</b>\n"
            f"Лимит изменений цены: <b>{rate}/мин</b>\n"
            f"Игнорируется продавцов: <b>{len(ign)}</b>\n\n"
            "Выберите лот для настройки или добавьте новый."
        )

    def kb_lot(lot_id: str) -> "K":
        lot = _get_lot_cfg(lot_id) or dict(DEFAULT_LOT)
        kb = K()
        kb.row(
            B(f"Мин: {_fmt(lot['min_price'])}", callback_data=f"{CB_SET}:min:{lot_id}"),
            B(f"Макс: {_fmt(lot['max_price'])}", callback_data=f"{CB_SET}:max:{lot_id}"),
        )
        kb.row(
            B(f"Шаг: {_fmt(lot['step'])}", callback_data=f"{CB_SET}:step:{lot_id}"),
            B(f"Интервал: {'%g' % _safe_interval(lot['interval'])}с",
              callback_data=f"{CB_SET}:int:{lot_id}"),
        )
        kb.add(B(f"Без конкурентов: {NO_COMP_TITLES.get(lot['no_competitor_strategy'])}",
                 callback_data=f"{CB_STRAT}:{lot_id}"))
        if lot["no_competitor_strategy"] == "custom":
            kb.add(B(f"Заданная цена: {_fmt(lot.get('custom_price', 0.0))}",
                     callback_data=f"{CB_SET}:cprice:{lot_id}"))
        # Фильтры конкурентов.
        kb.row(
            B(f"Доставка: {DELIVERY_TITLES.get(lot.get('delivery', 'all'))}",
              callback_data=f"{CB_DELIV}:{lot_id}"),
            B(f"Онлайн: {'да' if lot.get('online_only') else 'нет'}",
              callback_data=f"{CB_ONLINE}:{lot_id}"),
        )
        kb.row(
            B(f"Мин. отзывов: {int(lot.get('min_reviews', 0))}",
              callback_data=f"{CB_SET}:reviews:{lot_id}"),
            B(f"Фильтр текста: {lot.get('keyword') or '—'}",
              callback_data=f"{CB_SET}:kw:{lot_id}"),
        )
        kb.add(B(f"⚡ Агрессивный режим (всегда 1-й): "
                 f"{'ВКЛ' if lot.get('aggressive') else 'выкл'}",
                 callback_data=f"{CB_AGGR}:{lot_id}"))
        if lot.get("enabled"):
            kb.add(B("⏹ Остановить", callback_data=f"{CB_TOGGLE}:{lot_id}"))
        else:
            kb.add(B("▶️ Запустить", callback_data=f"{CB_TOGGLE}:{lot_id}"))
        kb.row(
            B("🔄 Обновить", callback_data=f"{CB_LOT}:{lot_id}"),
            B("🗑 Удалить", callback_data=f"{CB_DEL}:{lot_id}"),
        )
        kb.add(B("◀️ Назад", callback_data=f"{CBT.PLUGIN_SETTINGS}:{UUID}:0"))
        return kb

    def text_lot(lot_id: str) -> str:
        lot = _get_lot_cfg(lot_id)
        if not lot:
            return "Лот не найден."
        st = _get_stat(lot_id)
        sym = st.get("symbol", "₽")
        with _CFG_LOCK:
            mode = _CFG.get("price_mode", "seller")
        status = "🟢 Работает" if lot.get("enabled") else "🔴 Остановлен"
        cur = _fmt(st.get("current_price"), sym)
        comp = st.get("competitors", "—")
        valid = st.get("valid", "—")
        mincomp = _fmt(st.get("min_comp"), sym) if st.get("min_comp") is not None else "—"
        err = st.get("error")
        lines = [
            "┌─ <b>Автодэмпинг</b> ─",
            f"│ Lot ID: <code>{lot_id}</code>",
            f"│ Статус: {status}",
            f"│ Цены: <b>{PRICE_MODE_TITLES.get(mode)}</b>",
            "│",
            f"│ Минимальная цена: {_fmt(lot['min_price'], sym)}",
            f"│ Максимальная цена: {_fmt(lot['max_price'], sym)}",
            f"│ Шаг: {_fmt(lot['step'], sym)}",
            f"│ Интервал: {'%g' % _safe_interval(lot['interval'])} сек",
            "│",
            f"│ Текущая цена: {cur}",
            f"│ Конкурентов: {comp}",
            f"│ Подходящих: {valid}",
            f"│ Минимум конкурентов: {mincomp}",
        ]
        subcat_id = st.get("subcat_id")
        if subcat_id:
            sc_name = st.get("subcat_name") or ""
            lines.append("│")
            lines.append(f"│ Категория: <code>{subcat_id}</code> {sc_name}".rstrip())
        # Сводка фильтров.
        flt = [f"доставка «{DELIVERY_TITLES.get(lot.get('delivery', 'all'))}»"]
        if lot.get("online_only"):
            flt.append("только онлайн")
        if int(lot.get("min_reviews", 0)) > 0:
            flt.append(f"отзывов ≥ {int(lot['min_reviews'])}")
        if lot.get("keyword"):
            flt.append(f"текст «{lot['keyword']}»")
        lines.append(f"│ Фильтры: {', '.join(flt)}")
        if lot.get("aggressive"):
            lines.append("│ ⚡ Агрессивный режим: цена сразу на MIN")
        if err:
            lines.append(f"│ ⚠️ Ошибка: {err}")
        lines.append("└─")
        return "\n".join(lines)

    def kb_ignore() -> "K":
        kb = K()
        with _CFG_LOCK:
            ign = list(_CFG.get("ignored_sellers", []))
        for sid in ign:
            kb.add(B(f"❌ {sid}", callback_data=f"{CB_IGN_DEL}:{sid}"))
        kb.add(B("➕ Добавить продавца", callback_data=CB_IGN_ADD))
        kb.add(B("◀️ Назад", callback_data=f"{CBT.PLUGIN_SETTINGS}:{UUID}:0"))
        return kb

    def text_ignore() -> str:
        with _CFG_LOCK:
            ign = list(_CFG.get("ignored_sellers", []))
        body = "\n".join(f"• <code>{s}</code>" for s in ign) or "<i>список пуст</i>"
        return ("🚫 <b>Игнорируемые продавцы</b>\n\n"
                "Их лоты не учитываются при расчёте минимальной цены.\n\n" + body)

    def kb_quick() -> "K":
        kb = K()
        with _CFG_LOCK:
            lots = dict(_CFG["lots"])
        for lot_id, lot in lots.items():
            mark = "🟢" if lot.get("enabled") else "🔴"
            name = lot.get("keyword") or lot_id          # имя из «Фильтр текста»
            st = _get_stat(lot_id)
            price = st.get("current_price")
            sym = st.get("symbol", "₽")
            label = f"{mark} {name}"
            if price is not None:
                label += f" · {_fmt(price, sym)}"
            kb.add(B(label, callback_data=f"{CB_QTOGGLE}:{lot_id}"))
        kb.add(B("◀️ Назад", callback_data=f"{CBT.PLUGIN_SETTINGS}:{UUID}:0"))
        return kb

    def text_quick() -> str:
        with _CFG_LOCK:
            lots = _CFG["lots"]
            on = sum(1 for x in lots.values() if x.get("enabled"))
            total = len(lots)
        return ("⚡ <b>Быстрое вкл/выкл лотов</b>\n\n"
                f"Работает: <b>{on}</b> из <b>{total}</b>\n\n"
                "Нажмите на лот — включить/выключить автодемп.\n"
                "🟢 — работает, 🔴 — выключен.\n"
                "Название берётся из «Фильтр текста» (иначе Lot ID).")

    def _edit(call, text: str, kb) -> None:
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.id,
                                  reply_markup=kb, parse_mode="HTML")
        except Exception:
            bot.send_message(call.message.chat.id, text, reply_markup=kb, parse_mode="HTML")

    # ------------------------- обработчики ------------------------- #
    def open_settings(call: "CallbackQuery"):
        _edit(call, text_main(), kb_main())
        bot.answer_callback_query(call.id)

    def open_lot(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        _edit(call, text_lot(lot_id), kb_lot(lot_id))
        bot.answer_callback_query(call.id)

    def open_ignore(call: "CallbackQuery"):
        _edit(call, text_ignore(), kb_ignore())
        bot.answer_callback_query(call.id)

    def open_quick(call: "CallbackQuery"):
        _edit(call, text_quick(), kb_quick())
        bot.answer_callback_query(call.id)

    def quick_toggle(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            lot = _CFG["lots"].get(lot_id)
            if not lot:
                bot.answer_callback_query(call.id, "Лот не найден")
                return
            if not lot["enabled"]:
                if (lot["min_price"] <= 0 or lot["max_price"] <= 0
                        or lot["max_price"] < lot["min_price"]):
                    bot.answer_callback_query(
                        call.id, "Сначала задайте корректные Мин/Макс у лота!",
                        show_alert=True)
                    return
            lot["enabled"] = not lot["enabled"]
            enabled = lot["enabled"]
            name = lot.get("keyword") or lot_id
        save_config()
        if enabled:
            start_lot(cardinal, lot_id)
        else:
            stop_lot(lot_id, join=False)
        _edit(call, text_quick(), kb_quick())
        bot.answer_callback_query(
            call.id, f"{name}: {'▶️ включён' if enabled else '⏹ выключен'}")

    def toggle_mode(call: "CallbackQuery"):
        with _CFG_LOCK:
            cur = _CFG.get("price_mode", "seller")
            _CFG["price_mode"] = "buyer" if cur == "seller" else "seller"
            new = _CFG["price_mode"]
        save_config()
        _edit(call, text_main(), kb_main())
        bot.answer_callback_query(
            call.id, f"Режим цены: {PRICE_MODE_TITLES.get(new)}", show_alert=True)

    def toggle_fast(call: "CallbackQuery"):
        with _CFG_LOCK:
            _CFG["fast_check"] = not _CFG.get("fast_check", False)
            new = _CFG["fast_check"]
        save_config()
        _edit(call, text_main(), kb_main())
        bot.answer_callback_query(
            call.id,
            "Быстрая проверка ВКЛ: обход кэша FunPay (свежее цены, но выше "
            "нагрузка). Проверьте, стало ли быстрее." if new
            else "Быстрая проверка выключена",
            show_alert=True)

    def toggle_sync(call: "CallbackQuery"):
        with _CFG_LOCK:
            _CFG["sync_refresh"] = not _CFG.get("sync_refresh", False)
            new = _CFG["sync_refresh"]
        save_config()
        _edit(call, text_main(), kb_main())
        bot.answer_callback_query(
            call.id,
            "Синхронизация ВКЛ: плагин ловит период обновления таблицы и меняет "
            "цену прямо перед снимком — чтобы стоять первым. Дайте ~минуту на "
            "подстройку." if new else "Синхронизация с таблицей выключена",
            show_alert=True)

    def cycle_strategy(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            lot = _CFG["lots"].get(lot_id)
            if lot:
                idx = NO_COMP_STRATEGIES.index(lot["no_competitor_strategy"])
                lot["no_competitor_strategy"] = NO_COMP_STRATEGIES[(idx + 1) % len(NO_COMP_STRATEGIES)]
        save_config()
        _edit(call, text_lot(lot_id), kb_lot(lot_id))
        bot.answer_callback_query(call.id)

    def cycle_delivery(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            lot = _CFG["lots"].get(lot_id)
            if lot:
                idx = DELIVERY_MODES.index(lot.get("delivery", "all"))
                lot["delivery"] = DELIVERY_MODES[(idx + 1) % len(DELIVERY_MODES)]
        save_config()
        _edit(call, text_lot(lot_id), kb_lot(lot_id))
        bot.answer_callback_query(call.id)

    def toggle_online(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            lot = _CFG["lots"].get(lot_id)
            if lot:
                lot["online_only"] = not lot.get("online_only", False)
        save_config()
        _edit(call, text_lot(lot_id), kb_lot(lot_id))
        bot.answer_callback_query(call.id)

    def toggle_aggressive(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            lot = _CFG["lots"].get(lot_id)
            if lot:
                lot["aggressive"] = not lot.get("aggressive", False)
                new = lot["aggressive"]
        save_config()
        _edit(call, text_lot(lot_id), kb_lot(lot_id))
        bot.answer_callback_query(
            call.id,
            "Агрессивный режим включён: цена сразу опускается до MIN"
            if new else "Агрессивный режим выключен",
            show_alert=True)

    def toggle_lot(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            lot = _CFG["lots"].get(lot_id)
            if not lot:
                bot.answer_callback_query(call.id, "Лот не найден")
                return
            # Проверка валидности перед запуском.
            if not lot["enabled"]:
                if lot["min_price"] <= 0 or lot["max_price"] <= 0 or lot["max_price"] < lot["min_price"]:
                    bot.answer_callback_query(
                        call.id, "Укажите корректные мин. и макс. цены!", show_alert=True)
                    return
            lot["enabled"] = not lot["enabled"]
            enabled = lot["enabled"]
        save_config()
        if enabled:
            start_lot(cardinal, lot_id)
        else:
            stop_lot(lot_id, join=False)
        _edit(call, text_lot(lot_id), kb_lot(lot_id))
        bot.answer_callback_query(call.id, "Запущено" if enabled else "Остановлено")

    def confirm_delete(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        kb = K()
        kb.row(
            B("✅ Да, удалить", callback_data=f"{CB_DEL_OK}:{lot_id}"),
            B("◀️ Отмена", callback_data=f"{CB_LOT}:{lot_id}"),
        )
        _edit(call, f"Удалить лот <code>{lot_id}</code> из автодемпинга?", kb)
        bot.answer_callback_query(call.id)

    def do_delete(call: "CallbackQuery"):
        lot_id = call.data.split(":", 1)[1]
        stop_lot(lot_id, join=False)
        with _CFG_LOCK:
            _CFG["lots"].pop(lot_id, None)
            _STATS.pop(lot_id, None)
        with _META_LOCK:
            _LOT_META.pop(lot_id, None)
        with _REFRESH_LOCK:
            _REFRESH.pop(lot_id, None)
        save_config()
        _edit(call, text_main(), kb_main())
        bot.answer_callback_query(call.id, "Лот удалён")

    def del_ignore(call: "CallbackQuery"):
        sid = call.data.split(":", 1)[1]
        with _CFG_LOCK:
            try:
                _CFG["ignored_sellers"].remove(int(sid))
            except (ValueError, KeyError):
                pass
        save_config()
        _edit(call, text_ignore(), kb_ignore())
        bot.answer_callback_query(call.id)

    # ---- ввод значений через состояния ---- #
    def ask(call: "CallbackQuery", state: str, prompt: str):
        m = bot.send_message(call.message.chat.id, prompt, parse_mode="HTML")
        tg.set_state(m.chat.id, m.id, call.from_user.id, state,
                     {"card_mid": call.message.id})
        bot.answer_callback_query(call.id)

    def ask_add(call: "CallbackQuery"):
        ask(call, ST_ADD, "Отправьте <b>Lot ID</b> лота, который нужно демпинговать:")

    def ask_set(call: "CallbackQuery"):
        _, param, lot_id = call.data.split(":", 2)
        titles = {"min": "минимальную цену", "max": "максимальную цену",
                  "step": "шаг снижения", "int": "интервал проверки (сек, можно дробный)",
                  "cprice": "заданную цену (стратегия «без конкурентов»)",
                  "reviews": "минимум отзывов у конкурента (0 — без ограничения)",
                  "kw": "текст, который должен быть в описании конкурента "
                        "(отправьте «-» чтобы очистить)"}
        m = bot.send_message(call.message.chat.id,
                             f"Отправьте новое значение — {titles.get(param, param)}:")
        tg.set_state(m.chat.id, m.id, call.from_user.id, f"{ST_SET}:{param}:{lot_id}",
                     {"card_mid": call.message.id})
        bot.answer_callback_query(call.id)

    def ask_ignore_add(call: "CallbackQuery"):
        ask(call, ST_IGN_ADD,
            "Отправьте <b>ID продавца</b> (можно несколько через пробел/запятую):")

    def ask_rate(call: "CallbackQuery"):
        ask(call, ST_RATE,
            "Отправьте максимальное число <b>изменений цены в минуту</b> (например 10):")

    def ask_syncdelay(call: "CallbackQuery"):
        ask(call, ST_SYNCDELAY,
            "Через сколько <b>секунд после обновления таблицы</b> менять цену? "
            "(например 7; можно дробное)")

    def _refresh_card(chat_id: int, card_mid: int | None, text: str, kb):
        if card_mid is None:
            bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")
            return
        try:
            bot.edit_message_text(text, chat_id, card_mid, reply_markup=kb, parse_mode="HTML")
        except Exception:
            bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

    def handle_input(message: "Message"):
        st = tg.get_state(message.chat.id, message.from_user.id)
        if not st:
            return
        state = st["state"]
        data = st.get("data", {})
        card_mid = data.get("card_mid")
        text = (message.text or "").strip()
        tg.clear_state(message.chat.id, message.from_user.id)
        # Убираем сообщения ввода для чистоты чата (best-effort).
        for mid in (message.id, st.get("mid")):
            try:
                bot.delete_message(message.chat.id, mid)
            except Exception:
                pass

        def reply(t):
            bot.send_message(message.chat.id, t, parse_mode="HTML")

        if state == ST_ADD:
            # Принимаем как чистый ID, так и ссылку вида
            # https://funpay.com/lots/offer?id=75751254
            m = re.search(r"id=(\d+)", text)
            lot_id = m.group(1) if m else text.replace(" ", "")
            if not lot_id.isdigit():
                reply("❌ Отправьте числовой Lot ID (например 75751254) "
                      "или ссылку на лот. Попробуйте снова.")
                return
            with _CFG_LOCK:
                if lot_id in _CFG["lots"]:
                    reply("⚠️ Такой лот уже добавлен.")
                else:
                    _CFG["lots"][lot_id] = dict(DEFAULT_LOT)
            save_config()
            _refresh_card(message.chat.id, card_mid, text_lot(lot_id), kb_lot(lot_id))
            return

        if state.startswith(ST_SET):
            _, _, param, lot_id = state.split(":", 3)
            # Текстовый фильтр — принимаем строку, не число.
            if param == "kw":
                with _CFG_LOCK:
                    lot = _CFG["lots"].get(lot_id)
                    if not lot:
                        reply("Лот не найден.")
                        return
                    lot["keyword"] = "" if text in ("-", "") else text
                save_config()
                _refresh_card(message.chat.id, card_mid, text_lot(lot_id), kb_lot(lot_id))
                return
            value = text.replace(",", ".")
            try:
                num = float(value)
            except ValueError:
                reply("❌ Нужно число. Попробуйте снова.")
                return
            with _CFG_LOCK:
                lot = _CFG["lots"].get(lot_id)
                if not lot:
                    reply("Лот не найден.")
                    return
                if param == "reviews":
                    lot["min_reviews"] = max(0, int(num))
                elif param == "min":
                    lot["min_price"] = max(0.0, round(num, 2))
                elif param == "max":
                    lot["max_price"] = max(0.0, round(num, 2))
                elif param == "step":
                    lot["step"] = max(0.01, round(num, 2))
                elif param == "cprice":
                    lot["custom_price"] = max(0.0, round(num, 2))
                elif param == "int":
                    lot["interval"] = _safe_interval(num)
            save_config()
            _refresh_card(message.chat.id, card_mid, text_lot(lot_id), kb_lot(lot_id))
            return

        if state == ST_IGN_ADD:
            ids = [p for p in text.replace(",", " ").split() if p.strip().isdigit()]
            if not ids:
                reply("❌ Не найдено ни одного числового ID.")
                return
            with _CFG_LOCK:
                cur = set(_CFG.get("ignored_sellers", []))
                cur.update(int(i) for i in ids)
                _CFG["ignored_sellers"] = sorted(cur)
            save_config()
            _refresh_card(message.chat.id, card_mid, text_ignore(), kb_ignore())
            return

        if state == ST_RATE:
            if not text.isdigit() or int(text) < 1:
                reply("❌ Нужно целое число ≥ 1.")
                return
            with _CFG_LOCK:
                _CFG["max_changes_per_min"] = int(text)
            save_config()
            _refresh_card(message.chat.id, card_mid, text_main(), kb_main())
            return

        if state == ST_SYNCDELAY:
            try:
                val = float(text.replace(",", "."))
            except ValueError:
                reply("❌ Нужно число (секунды), например 7.")
                return
            with _CFG_LOCK:
                _CFG["sync_delay"] = max(0.0, val)
            save_config()
            _refresh_card(message.chat.id, card_mid, text_main(), kb_main())
            return

    # ------------------------- регистрация ------------------------- #
    tg.cbq_handler(open_settings,
                   lambda c: c.data.startswith(f"{CBT.PLUGIN_SETTINGS}:{UUID}"))
    tg.cbq_handler(open_lot, lambda c: c.data.startswith(f"{CB_LOT}:"))
    tg.cbq_handler(ask_add, lambda c: c.data == CB_ADD)
    tg.cbq_handler(ask_set, lambda c: c.data.startswith(f"{CB_SET}:"))
    tg.cbq_handler(cycle_strategy, lambda c: c.data.startswith(f"{CB_STRAT}:"))
    tg.cbq_handler(cycle_delivery, lambda c: c.data.startswith(f"{CB_DELIV}:"))
    tg.cbq_handler(toggle_online, lambda c: c.data.startswith(f"{CB_ONLINE}:"))
    tg.cbq_handler(toggle_aggressive, lambda c: c.data.startswith(f"{CB_AGGR}:"))
    tg.cbq_handler(toggle_lot, lambda c: c.data.startswith(f"{CB_TOGGLE}:"))
    tg.cbq_handler(confirm_delete, lambda c: c.data.startswith(f"{CB_DEL}:"))
    tg.cbq_handler(do_delete, lambda c: c.data.startswith(f"{CB_DEL_OK}:"))
    tg.cbq_handler(open_ignore, lambda c: c.data == CB_IGN)
    tg.cbq_handler(open_quick, lambda c: c.data == CB_QUICK)
    tg.cbq_handler(quick_toggle, lambda c: c.data.startswith(f"{CB_QTOGGLE}:"))
    tg.cbq_handler(ask_ignore_add, lambda c: c.data == CB_IGN_ADD)
    tg.cbq_handler(del_ignore, lambda c: c.data.startswith(f"{CB_IGN_DEL}:"))
    tg.cbq_handler(ask_rate, lambda c: c.data == CB_RATE)
    tg.cbq_handler(toggle_mode, lambda c: c.data == CB_MODE)
    tg.cbq_handler(toggle_fast, lambda c: c.data == CB_FAST)
    tg.cbq_handler(toggle_sync, lambda c: c.data == CB_SYNC)
    tg.cbq_handler(ask_syncdelay, lambda c: c.data == CB_SYNCDELAY)

    tg.msg_handler(
        handle_input,
        func=lambda m: (
            (s := tg.get_state(m.chat.id, m.from_user.id)) is not None
            and isinstance(s.get("state"), str)
            and s["state"].startswith("AD:")
        ),
    )
    logger.info(f"{LOGGER_PREFIX} Интерфейс настроек Telegram зарегистрирован.")


# --------------------------------------------------------------------------- #
#                        Точки входа плагина FunPayCardinal                    #
# --------------------------------------------------------------------------- #
def init_commands(cardinal: "Cardinal", *args) -> None:
    """PRE_INIT: регистрация Telegram-интерфейса."""
    global _CARDINAL
    _CARDINAL = cardinal
    _register_telegram(cardinal)


def init(cardinal: "Cardinal", *args) -> None:
    """POST_INIT: загрузка конфига и запуск воркеров для активных лотов."""
    global _CARDINAL
    _CARDINAL = cardinal
    load_config()
    with _CFG_LOCK:
        active = [lid for lid, lot in _CFG["lots"].items() if lot.get("enabled")]
    for lot_id in active:
        start_lot(cardinal, lot_id)
    logger.info(
        f"{LOGGER_PREFIX} Плагин загружен. Активных лотов: {len(active)}."
    )


def delete(*args) -> None:
    """BIND_TO_DELETE: корректная остановка при удалении плагина."""
    stop_all(join=True)
    logger.info(f"{LOGGER_PREFIX} Плагин выгружен, все воркеры остановлены.")


BIND_TO_PRE_INIT = [init_commands]
BIND_TO_POST_INIT = [init]
BIND_TO_DELETE = delete
