"""
告警管理器。

检测三类异常：
  1. 连接断开     — WebSocket 断线超过 N 秒未恢复
  2. 延迟异常     — P95 延迟连续超过阈值
  3. 写库失败     — 写入错误数超过阈值

通知渠道（按优先级）：
  1. Telegram Bot（实时手机推送）— 需配置 TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
  2. 告警文件     — 始终写入 /opt/lmr-hunter/logs/alerts.log
  3. 标准日志     — 始终输出，不依赖任何配置

告警防抖：同类告警 10 分钟内不重复发送，避免刷屏。
"""
from __future__ import annotations

import asyncio
import time
from enum import Enum
from pathlib import Path

import aiohttp

from src.utils.logger import setup_logger

logger = setup_logger("monitor.alerts")

# 告警防抖时间（秒）
_DEBOUNCE_SEC = 600


class AlertLevel(Enum):
    INFO     = "INFO"
    WARN     = "WARN"
    ERROR    = "ERROR"
    CRITICAL = "CRITICAL"


# 各级别对应的 emoji（Telegram 消息可读性）
_EMOJI = {
    AlertLevel.INFO:     "ℹ️",
    AlertLevel.WARN:     "⚠️",
    AlertLevel.ERROR:    "🔴",
    AlertLevel.CRITICAL: "🚨",
}


class AlertManager:
    """
    告警管理器，集成到 main.py 的统计周期中调用。

    使用方式：
        alert = AlertManager(
            telegram_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            alert_log="/opt/lmr-hunter/logs/alerts.log",
            latency_p95_threshold_ms=400,
            write_error_threshold=5,
        )
        # 在 stats_reporter 中调用：
        await alert.check_all(gateway_stats, latency_percentiles, writer_counts)
        # 在 gateway 断线/恢复时调用：
        await alert.on_disconnect()
        await alert.on_reconnect()
    """

    def __init__(
        self,
        telegram_token:          str | None = None,
        telegram_chat_id:        str | None = None,
        alert_log:               str        = "/opt/lmr-hunter/logs/alerts.log",
        latency_p95_threshold_ms: float     = 400.0,
        write_error_threshold:   int        = 5,
        disconnect_timeout_sec:  int        = 120,
    ) -> None:
        self._token          = telegram_token
        self._chat_id        = telegram_chat_id
        self._alert_log      = Path(alert_log)
        self._alert_log.parent.mkdir(parents=True, exist_ok=True)

        self._lat_threshold  = latency_p95_threshold_ms
        self._err_threshold  = write_error_threshold
        self._dc_timeout     = disconnect_timeout_sec

        # 防抖：{alert_key: last_sent_ts}
        self._last_sent: dict[str, float] = {}

        # 断线追踪
        self._disconnected_at: float | None = None
        self._is_connected     = True

        tg = "已配置" if (telegram_token and telegram_chat_id) else "未配置"
        logger.info("AlertManager 初始化 | Telegram=%s | P95阈值=%.0fms | 写库错误阈值=%d",
                    tg, latency_p95_threshold_ms, write_error_threshold)

    # ── 网关事件 ──────────────────────────────────────────────────────────────

    async def on_disconnect(self) -> None:
        """WebSocket 断线时调用。"""
        if not self._is_connected:
            return
        self._is_connected    = False
        self._disconnected_at = time.time()
        logger.warning("告警触发: WebSocket 断线")

    async def on_reconnect(self) -> None:
        """WebSocket 重连成功时调用。"""
        if self._is_connected:
            return
        down_sec = int(time.time() - (self._disconnected_at or time.time()))
        self._is_connected    = True
        self._disconnected_at = None
        await self._send(
            key="reconnect",
            level=AlertLevel.INFO,
            title="WebSocket 已恢复",
            body=f"断线时长 {down_sec}s，已重新连接并恢复数据流。",
        )

    async def on_storage_degraded(self, discarded: int) -> None:
        """存储层进入 DEGRADED 状态时调用（带 10 分钟防抖）。"""
        await self._send(
            key="storage_degraded",
            level=AlertLevel.CRITICAL,
            title="存储层 DEGRADED",
            body=(
                f"批量写入持续失败，已丢弃 {discarded} 条成交样本。"
                f"研究数据已不完整，请检查磁盘和 SQLite 文件。"
            ),
        )

    # ── 周期性检测（在 stats_reporter 中调用）─────────────────────────────────

    async def check_all(
        self,
        gateway_stats:       dict[str, int],
        latency_percentiles: dict[str, float | None],
        writer_counts:       dict[str, int],
    ) -> None:
        """一次性运行所有告警检测。"""
        await self._check_disconnect()
        await self._check_latency(latency_percentiles)
        await self._check_write_errors(writer_counts)
        await self._check_data_stale(gateway_stats)

    # ── 内部检测 ──────────────────────────────────────────────────────────────

    async def _check_disconnect(self) -> None:
        """断线超过 disconnect_timeout_sec 未恢复则告警。"""
        if self._is_connected or self._disconnected_at is None:
            return
        down_sec = time.time() - self._disconnected_at
        if down_sec >= self._dc_timeout:
            await self._send(
                key="disconnect_timeout",
                level=AlertLevel.CRITICAL,
                title="WebSocket 长时间断线",
                body=f"已断线 {int(down_sec)}s，超过阈值 {self._dc_timeout}s。请检查 VPS 网络或 Binance 服务状态。",
            )

    async def _check_latency(self, lat: dict[str, float | None]) -> None:
        """P95 延迟超过阈值时告警。"""
        p95 = lat.get("p95")
        if p95 is None:
            return
        if p95 > self._lat_threshold:
            await self._send(
                key="latency_high",
                level=AlertLevel.WARN,
                title="延迟异常",
                body=f"P95={p95:.0f}ms 超过阈值 {self._lat_threshold:.0f}ms。"
                     f"P50={lat.get('p50', 'N/A')}ms P99={lat.get('p99', 'N/A')}ms",
            )

    async def _check_write_errors(self, counts: dict[str, int]) -> None:
        """写库错误数超过阈值时告警。"""
        errors = counts.get("errors", 0)
        if errors >= self._err_threshold:
            await self._send(
                key="write_error",
                level=AlertLevel.ERROR,
                title="数据库写入失败",
                body=f"累计写库错误 {errors} 次，超过阈值 {self._err_threshold}。"
                     f"数据可能丢失，请检查磁盘空间和 SQLite 文件。",
            )

    async def _check_data_stale(self, stats: dict[str, int]) -> None:
        """成交数据长时间为 0 可能是静默断线。"""
        # 用于检测"连接看起来正常但实际没有数据"的场景
        # 仅记录到日志，不发告警（成交量极低时可能误报）
        trades = stats.get("trades", 0)
        if trades == 0 and self._is_connected:
            logger.warning("数据异常：WebSocket 显示已连接但成交数为 0，可能是静默断线")

    # ── 发送告警 ──────────────────────────────────────────────────────────────

    async def _send(
        self,
        key:   str,
        level: AlertLevel,
        title: str,
        body:  str,
    ) -> None:
        """发送告警，带防抖保护。"""
        now = time.time()
        last = self._last_sent.get(key, 0)
        if now - last < _DEBOUNCE_SEC:
            return  # 防抖，跳过

        self._last_sent[key] = now
        emoji = _EMOJI[level]
        ts    = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        msg   = f"{emoji} [{level.value}] LMR-Hunter\n{title}\n{body}\n{ts}"

        # 始终写到告警文件
        self._write_alert_file(msg)

        # 始终输出到日志
        log_fn = {
            AlertLevel.INFO:     logger.info,
            AlertLevel.WARN:     logger.warning,
            AlertLevel.ERROR:    logger.error,
            AlertLevel.CRITICAL: logger.critical,
        }[level]
        log_fn("告警: %s | %s", title, body)

        # Telegram（可选）
        if self._token and self._chat_id:
            await self._send_telegram(msg)

    def _write_alert_file(self, msg: str) -> None:
        try:
            with self._alert_log.open("a", encoding="utf-8") as f:
                f.write(msg + "\n" + "-" * 40 + "\n")
        except Exception as e:
            logger.error("写告警文件失败: %s", e)

    async def _send_telegram(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json={
                    "chat_id":    self._chat_id,
                    "text":       text,
                    "parse_mode": "HTML",
                }) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning("Telegram 发送失败: status=%d body=%s", resp.status, body[:200])
        except asyncio.TimeoutError:
            logger.warning("Telegram 发送超时（5s）")
        except Exception as e:
            logger.warning("Telegram 发送异常: %s", e)
