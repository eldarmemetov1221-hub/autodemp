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

# Значения по умолчанию для нового лота.
DEFAULT_LOT = {
    "enabled": False,          # запущен ли демпинг по этому лоту
    "min_price": 0.0,          # нижняя граница (никогда не опускаемся ниже)
    "max_price": 0.0,          # верхняя граница диапазона учёта конкурентов
    "step": 0.01,              # на сколько опускаемся ниже конкурента
    "interval": 5.0,           # период проверки, сек (поддерживает дробные)
    "no_competitor_strategy": "keep",  # keep | max | custom
    "custom_price": 0.0,       # цена для стратегии "custom"
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

# Значения по умолчанию для всего плагина.
DEFAULT_CONFIG = {
    "max_changes_per_min": 10,   # защита от ценовой войны
    "ignored_sellers": [],       # id продавцов, чьи лоты игнорируются
    "lots": {},                  # {lot_id(str): {...}}
}

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
        lots = _call_with_retry(
            account.get_subcategory_public_lots, subcat.type, subcat.id, stop=stop
        )
        with _COMP_CACHE_LOCK:
            _COMP_CACHE[key] = (time.time(), lots)
        return lots


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

    # Блокировка исключает одновременное изменение одного и того же лота.
    lock = _lot_lock(lot_id)
    if not lock.acquire(blocking=False):
        # Другой цикл уже работает с этим лотом — пропускаем.
        return
    try:
        # 1. Текущая цена и поля лота (одним запросом, переиспользуем для save).
        lf = _call_with_retry(account.get_lot_fields, int(lot_id), stop=stop)
        current_price = lf.price
        symbol = _currency_symbol(getattr(lf, "currency", None))

        subcat = getattr(lf, "subcategory", None)
        if subcat is None:
            logger.error(f"{LOGGER_PREFIX} Лот {lot_id}: не удалось определить подкатегорию.")
            _set_stat(lot_id, error="нет подкатегории", updated=time.time())
            return

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
            filtered.append(c)

        # 4. Подходят по диапазону [min_price, max_price].
        valid = [c for c in filtered
                 if c.price is not None and min_price <= c.price <= max_price]

        total_found = len(filtered)
        valid_count = len(valid)
        min_comp = min((c.price for c in valid), default=None)

        # 5. Целевая цена.
        if valid:
            target = round(min_comp - step, 2)
            if target < min_price:
                target = round(min_price, 2)     # защита: не ниже MIN PRICE
        else:
            # Нет подходящих конкурентов — применяем выбранную стратегию.
            if strategy == "max":
                target = round(max_price, 2) if max_price > 0 else None
            elif strategy == "custom":
                target = round(custom_price, 2) if custom_price > 0 else None
            else:  # keep (по умолчанию) — не менять цену
                target = None
            # Стратегийную цену тоже держим в пределах [min_price, max_price].
            if target is not None:
                if min_price > 0:
                    target = max(target, round(min_price, 2))
                if max_price > 0:
                    target = min(target, round(max_price, 2))

        _set_stat(
            lot_id,
            current_price=current_price, competitors=total_found,
            valid=valid_count, min_comp=min_comp, target=target,
            symbol=symbol, error=None, updated=time.time(),
        )

        header = (
            f"Lot {lot_id}\n"
            f"Найдены конкуренты: {total_found}\n"
            f"Подходят по диапазону: {valid_count}\n"
            f"Минимальная цена конкурента: {_fmt(min_comp, symbol)}\n"
            f"Текущая цена: {_fmt(current_price, symbol)}"
        )

        # 6. Меняем цену, если нужно.
        if target is None or _prices_equal(target, current_price):
            logger.info(f"{LOGGER_PREFIX} {header}\nЦена уже оптимальна: "
                        f"{_fmt(current_price, symbol)}")
            return

        # Защита от ценовой войны — лимит изменений в минуту.
        if not _rate_allow():
            logger.warning(
                f"{LOGGER_PREFIX} {header}\nНовая цена: {_fmt(target, symbol)}\n"
                f"Пропуск: превышен лимит изменений цены в минуту "
                f"({_CFG.get('max_changes_per_min')})."
            )
            return

        try:
            lf.price = float(target)
            # renew_fields внутри save_lot синхронизирует поля; дублируем явно.
            try:
                lf.fields["price"] = str(target)
            except Exception:
                pass
            _call_with_retry(account.save_lot, lf, stop=stop)
            _rate_register()
            _set_stat(lot_id, current_price=target, updated=time.time())
            logger.info(
                f"{LOGGER_PREFIX} {header}\nНовая цена: {_fmt(target, symbol)}\n"
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

        interval = _safe_interval(lot_cfg.get("interval", MIN_SAFE_INTERVAL))
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

# Состояния ввода (msg_handler по префиксу "AD:").
ST_ADD = "AD:add"
ST_SET = "AD:set"        # AD:set:<param>:<lot_id>
ST_IGN_ADD = "AD:igadd"
ST_RATE = "AD:rate"


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
        kb.add(B("➕ Добавить лот", callback_data=CB_ADD))
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
        return (
            "🤖 <b>AutoDemp — автодемпинг</b>\n\n"
            f"Лотов настроено: <b>{len(lots)}</b>\n"
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
                  "cprice": "заданную цену (стратегия «без конкурентов»)"}
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
                if param == "min":
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

    # ------------------------- регистрация ------------------------- #
    tg.cbq_handler(open_settings,
                   lambda c: c.data.startswith(f"{CBT.PLUGIN_SETTINGS}:{UUID}"))
    tg.cbq_handler(open_lot, lambda c: c.data.startswith(f"{CB_LOT}:"))
    tg.cbq_handler(ask_add, lambda c: c.data == CB_ADD)
    tg.cbq_handler(ask_set, lambda c: c.data.startswith(f"{CB_SET}:"))
    tg.cbq_handler(cycle_strategy, lambda c: c.data.startswith(f"{CB_STRAT}:"))
    tg.cbq_handler(toggle_lot, lambda c: c.data.startswith(f"{CB_TOGGLE}:"))
    tg.cbq_handler(confirm_delete, lambda c: c.data.startswith(f"{CB_DEL}:"))
    tg.cbq_handler(do_delete, lambda c: c.data.startswith(f"{CB_DEL_OK}:"))
    tg.cbq_handler(open_ignore, lambda c: c.data == CB_IGN)
    tg.cbq_handler(ask_ignore_add, lambda c: c.data == CB_IGN_ADD)
    tg.cbq_handler(del_ignore, lambda c: c.data.startswith(f"{CB_IGN_DEL}:"))
    tg.cbq_handler(ask_rate, lambda c: c.data == CB_RATE)

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
