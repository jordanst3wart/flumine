import re
import logging
import hashlib
from typing import Optional, Tuple, Callable, Union
from decimal import Decimal, ROUND_HALF_UP

from betfairlightweight.compat import json
from betfairlightweight.resources import (
    MarketBook,
    MarketCatalogue,
    MarketDefinition,
    RunnerBook,
    Race,
    CricketMatch,
)

from . import config
from .exceptions import FlumineException

logger = logging.getLogger(__name__)

CUTOFFS = (
    (2, 100),
    (3, 50),
    (4, 20),
    (6, 10),
    (10, 5),
    (20, 2),
    (30, 1),
    (50, 0.5),
    (100, 0.2),
    (1000, 0.1),
)
MIN_PRICE = 1.01
MAX_PRICE = 1000
MARKET_ID_REGEX = re.compile(r"1.\d{9}")
EVENT_ID_REGEX = re.compile(r"\d{8}")
STRATEGY_NAME_HASH_LENGTH = 13


def chunks(l: list, n: int) -> list:
    for i in range(0, len(l), n):
        yield l[i : i + n]


def create_cheap_hash(txt: str, length: int = 15) -> str:
    # This is just a hash for debugging purposes.
    #    It does not need to be unique, just fast and short.
    # https://stackoverflow.com/questions/14023350
    hash_ = hashlib.sha1()
    hash_.update(txt.encode())
    return hash_.hexdigest()[:length]


def as_dec(value):
    return Decimal(str(value))


def arange(start, stop, step):
    while start < stop:
        yield start
        start += step


def make_prices(min_price, cutoffs):
    prices = []
    cursor = as_dec(min_price)
    for cutoff, step in cutoffs:
        prices.extend(arange(as_dec(cursor), as_dec(cutoff), as_dec(1 / step)))
        cursor = cutoff
    prices.append(as_dec(MAX_PRICE))
    return prices


def make_line_prices(min_unit: float, max_unit: float, interval: float) -> list:
    prices = [min_unit]
    price = min_unit
    while True:
        price += interval
        if price > max_unit:
            return prices
        prices.append(price)


PRICES = make_prices(MIN_PRICE, CUTOFFS)
PRICES_FLOAT = [float(price) for price in PRICES]
FINEST_PRICES = make_prices(MIN_PRICE, ((1000, 100),))


def get_price(data: list, level: int) -> Optional[float]:
    try:
        return data[level]["price"]
    except KeyError:
        return
    except IndexError:
        return
    except TypeError:
        return


def get_size(data: list, level: int) -> Optional[float]:
    try:
        return data[level]["size"]
    except KeyError:
        return
    except IndexError:
        return
    except TypeError:
        return


def get_sp(runner: RunnerBook) -> Optional[float]:
    if isinstance(runner.sp, list):
        return
    elif runner.sp is None:
        return
    elif runner.sp.actual_sp == "NaN":
        return
    elif runner.sp.actual_sp == "Infinity":
        return
    else:
        return runner.sp.actual_sp


def calculate_matched_exposure(mb: list, ml: list) -> Tuple:
    """Calculates exposure based on list
    of (price, size)
    returns the tuple (profit_if_win, profit_if_lose)
    """
    if not mb and not ml:
        return 0.0, 0.0
    back_exp, back_profit, lay_exp, lay_profit = 0, 0, 0, 0
    for p, s in mb:
        back_exp += -s
        back_profit += (p - 1) * s
    for p, s in ml:
        lay_exp += (p - 1) * -s
        lay_profit += s
    _win = back_profit + lay_exp
    _lose = lay_profit + back_exp
    return round(_win, 2), round(_lose, 2)


def calculate_unmatched_exposure(ub: list, ul: list) -> Tuple:
    """Calculates worse-case exposure based on list
    of (price, size)
    returns the tuple (profit_if_win, profit_if_lose)

    The worst case profit_if_win arises if all lay bets are matched and the selection wins.
    The worst case profit_if_lose arises if all back bets are matched and the selection loses.

    """
    if not ub and not ul:
        return 0.0, 0.0
    back_exp, lay_exp = 0, 0
    for p, s in ub:
        back_exp += -s
    for p, s in ul:
        lay_exp += (p - 1) * -s
    return round(lay_exp, 2), round(back_exp, 2)


def wap(matched: list) -> Tuple[float, float]:
    if not matched:
        return 0, 0
    a, b = 0, 0
    for _, p, s in matched:
        a += p * s
        b += s
    if b == 0 or a == 0:
        return 0, 0
    else:
        return round(b, 2), round(a / b, 2)


def call_strategy_error_handling(
    func: Callable,
    market,
    update: Union[MarketBook, MarketCatalogue, Race, CricketMatch],
) -> Optional[bool]:
    try:
        return func(market, update)
    except FlumineException as e:
        logger.error(
            "FlumineException %s in %s (%s)",
            e,
            func.__name__,
            market.market_id,
            exc_info=True,
        )
    except Exception as e:
        logger.critical(
            "Unknown error %s in %s (%s)",
            e,
            func.__name__,
            market.market_id,
            exc_info=True,
        )
        if config.raise_errors:
            raise
    return False


def call_middleware_error_handling(middleware, market) -> None:
    try:
        middleware(market)
    except FlumineException as e:
        logger.error(
            "FlumineException %s in %s (%s)",
            e,
            middleware,
            market.market_id,
            exc_info=True,
        )
    except Exception as e:
        logger.critical(
            "Unknown error %s in %s (%s)",
            e,
            middleware,
            market.market_id,
            exc_info=True,
        )
        if config.raise_errors:
            raise


def call_process_orders_error_handling(strategy, market, strategy_orders: list) -> None:
    try:
        strategy.process_orders(market, strategy_orders)
    except FlumineException as e:
        logger.error(
            "FlumineException %s in %s (%s)",
            e,
            strategy,
            market.market_id,
            exc_info=True,
        )
    except Exception as e:
        logger.critical(
            "Unknown error %s in %s (%s)",
            e,
            strategy,
            market.market_id,
            exc_info=True,
        )
        if config.raise_errors:
            raise
