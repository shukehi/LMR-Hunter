"""
标准化后的市场数据模型。
所有字段在解析时即完成类型转换，下游模块只消费强类型数据。
"""
from __future__ import annotations

from dataclasses import dataclass

# BUG-19 修复：时间戳合理性边界
# 下限：2020-01-01 00:00:00 UTC（ms），拒绝明显错误的历史值
_TS_MIN_MS: int = 1_577_836_800_000
# 未来容忍：允许交易所时钟比本地快最多 10 秒（正常 NTP 偏差范围之外即拒绝）
_TS_FUTURE_TOLERANCE_MS: int = 10_000


def _check_ts(field: str, event_ts: int, recv_ts: int) -> None:
    """校验交易所时间戳合理性，不合法时抛 ValueError（由 _listen 计入 parse_errors）。"""
    if event_ts <= 0:
        raise ValueError(f"{field}={event_ts} 非正数")
    if event_ts < _TS_MIN_MS:
        raise ValueError(f"{field}={event_ts} 早于 2020-01-01，疑似数据异常")
    if event_ts > recv_ts + _TS_FUTURE_TOLERANCE_MS:
        raise ValueError(
            f"{field}={event_ts} 比本地接收时间 {recv_ts} "
            f"早了 {event_ts - recv_ts}ms，超出时钟容忍范围"
        )


@dataclass(slots=True)
class Liquidation:
    """强平事件（forceOrder）"""
    recv_ts: int        # 本地收到时间戳 (ms)
    event_ts: int       # 交易所事件时间戳 (ms)
    symbol: str         # 交易对，如 BTCUSDT
    side: str           # SELL=多头被强平, BUY=空头被强平
    qty: float          # 强平数量 (BTC)
    price: float        # 成交均价 (USDT)
    notional: float     # 名义价值 = qty * price (USDT)
    status: str         # FILLED | PARTIALLY_FILLED

    @classmethod
    def from_raw(cls, data: dict, recv_ts: int) -> Liquidation:
        o = data["o"]
        qty = float(o["q"])
        price = float(o["ap"]) or float(o["p"])
        event_ts = int(data["E"])
        _check_ts("Liquidation.event_ts", event_ts, recv_ts)
        return cls(
            recv_ts=recv_ts,
            event_ts=event_ts,
            symbol=o["s"],
            side=o["S"],
            qty=qty,
            price=price,
            notional=round(qty * price, 2),
            status=o["X"],
        )


@dataclass(slots=True)
class Trade:
    """聚合成交（aggTrade）"""
    recv_ts: int        # 本地收到时间戳 (ms)
    trade_ts: int       # 成交时间戳 (ms)
    symbol: str
    price: float
    qty: float
    is_buyer_maker: bool  # True=主动卖, False=主动买
    agg_trade_id: int = 0  # aggTrade 序列号（"a" 字段），严格递增，用于检测漏采

    @classmethod
    def from_raw(cls, data: dict, recv_ts: int) -> Trade:
        trade_ts = int(data["T"])
        _check_ts("Trade.trade_ts", trade_ts, recv_ts)
        return cls(
            recv_ts=recv_ts,
            trade_ts=trade_ts,
            symbol=data["s"],
            price=float(data["p"]),
            qty=float(data["q"]),
            is_buyer_maker=bool(data["m"]),
            agg_trade_id=int(data.get("a", 0)),
        )


@dataclass(slots=True)
class Depth:
    """盘口深度快照（depth20@100ms）"""
    recv_ts: int        # 本地收到时间戳 (ms)
    event_ts: int       # 交易所事件时间戳 (ms)
    symbol: str
    bids: list[tuple[float, float]]  # [(price, qty), ...] 由高到低
    asks: list[tuple[float, float]]  # [(price, qty), ...] 由低到高

    @classmethod
    def from_raw(cls, data: dict, recv_ts: int) -> Depth:
        event_ts = int(data["E"])
        _check_ts("Depth.event_ts", event_ts, recv_ts)
        return cls(
            recv_ts=recv_ts,
            event_ts=event_ts,
            symbol=data["s"],
            bids=[(float(p), float(q)) for p, q in data["b"]],
            asks=[(float(p), float(q)) for p, q in data["a"]],
        )

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        return (self.best_bid + self.best_ask) / 2

    @property
    def bid_depth_usdt(self) -> float:
        """买盘总深度 (USDT) — 所有 bid 档位的名义价值之和，用于 Impact Ratio 计算。"""
        return sum(price * qty for price, qty in self.bids)


@dataclass(slots=True)
class Kline:
    """1 分钟 K 线（kline_1m）"""
    recv_ts:   int    # 本地收到时间戳 (ms)
    open_ts:   int    # K 线开盘时间 (ms)
    close_ts:  int    # K 线收盘时间 (ms)
    symbol:    str
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float  # 成交量 (BTC)
    is_closed: bool   # True=已收盘

    @property
    def typical_price(self) -> float:
        """典型价格 = (high + low + close) / 3，用于 VWAP 计算。"""
        return (self.high + self.low + self.close) / 3

    @classmethod
    def from_raw(cls, data: dict, recv_ts: int) -> "Kline":
        k = data["k"]
        open_ts = int(k["t"])
        _check_ts("Kline.open_ts", open_ts, recv_ts)
        return cls(
            recv_ts=recv_ts,
            open_ts=open_ts,
            close_ts=int(k["T"]),
            symbol=k["s"],
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            is_closed=bool(k["x"]),
        )

    @classmethod
    def from_rest(cls, row: list, symbol: str, recv_ts: int) -> "Kline":
        """
        从 REST API 返回的 K 线数组构建（冷启动引导用）。

        is_closed 由 close_ts vs recv_ts 推断：
        - close_ts < recv_ts → 已收盘（过去的 K 线）
        - close_ts >= recv_ts → 当前开放 K 线（最后一根）
        不强制设为 True，避免将当前未收盘 K 线误标，污染 VWAP 计算。
        """
        open_ts  = int(row[0])
        close_ts = int(row[6])
        _check_ts("Kline.open_ts(rest)", open_ts, recv_ts)
        return cls(
            recv_ts=recv_ts,
            open_ts=open_ts,
            close_ts=close_ts,
            symbol=symbol,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            is_closed=close_ts < recv_ts,
        )


@dataclass(slots=True)
class MarkPrice:
    """标记价格事件（markPrice@1s）— 提供现货指数价格作为策略核心锚点。"""
    recv_ts: int          # 本地收到时间戳 (ms)
    event_ts: int         # 交易所事件时间戳 (ms)
    symbol: str
    mark_price: float     # 标记价格
    index_price: float    # 现货指数价格（策略核心：公允价值锚点）
    funding_rate: float   # 当前资金费率
    next_funding_ts: int  # 下次资金费率时间

    @classmethod
    def from_raw(cls, data: dict, recv_ts: int) -> MarkPrice:
        event_ts = int(data["E"])
        _check_ts("MarkPrice.event_ts", event_ts, recv_ts)
        return cls(
            recv_ts=recv_ts,
            event_ts=event_ts,
            symbol=data["s"],
            mark_price=float(data["p"]),
            index_price=float(data["i"]),
            funding_rate=float(data.get("r", "0")),
            next_funding_ts=int(data.get("T", 0)),
        )
