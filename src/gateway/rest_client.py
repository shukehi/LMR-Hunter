"""
Binance Futures REST 客户端。

当前用途：启动时拉取最近 N 根 1m K 线，为 VWAP 计算提供冷启动数据。
         数据审计：拉取历史强平订单，与 WebSocket 采集结果交叉核对。
后续阶段：下单/撤单/查询也通过此模块实现。
"""
from __future__ import annotations

import time

import aiohttp

from src.gateway.models import Kline
from src.utils.logger import setup_logger

logger = setup_logger("gateway.rest")

_BASE_URL = "https://fapi.binance.com"
_TIMEOUT = aiohttp.ClientTimeout(total=10)

# VWAP 计算所需的最少已收盘 K 线数。fetch_klines 默认多拉 1 根以确保
# 在边界时刻（当前 K 线恰好收盘）也能满足要求。
# 注意：如果修改 calculator.py 中 VWAP 的 K 线数量，需同步修改此常量。
VWAP_KLINE_LOOKBACK = 15


async def fetch_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = VWAP_KLINE_LOOKBACK + 1,  # +1 确保过滤未收盘 K 线后仍有足够数据
) -> list[Kline]:
    """
    拉取最近 `limit` 根已收盘 K 线。

    返回按时间升序排列的 Kline 列表，长度通常为 limit-1
    （最后一根未收盘的会被过滤掉，除非恰好在收盘时拉取）。
    """
    url = f"{_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    recv_ts = int(time.time() * 1000)

    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                rows = await resp.json()

        klines = [Kline.from_rest(row, symbol.upper(), recv_ts) for row in rows]

        # 过滤掉未收盘的最后一根（避免污染 VWAP 基线）
        closed = [k for k in klines if k.is_closed]

        logger.info(
            "REST 引导 K 线: 拉取 %d 根，已收盘 %d 根 | symbol=%s",
            len(klines), len(closed), symbol,
        )
        return closed

    except aiohttp.ClientError as e:
        logger.error("REST 请求失败: %s | url=%s params=%s", e, url, params)
        return []
    except Exception as e:
        logger.error("REST 解析失败: %s", e, exc_info=True)
        return []
