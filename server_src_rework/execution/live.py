"""Real Polymarket execution via polymarket-client (`polymarket`), Polymarket's
official SDK.

`py_clob_client_v2` was retired for live trading here because its
`create_or_derive_api_key()` binds L1 auth to the raw signing EOA via plain
ECDSA recovery -- it never authenticates as the Deposit Wallet. Every order
placed with a Deposit Wallet as `maker`/`signer` (the only wallet type
Polymarket's backend now accepts, see docs.polymarket.com/trading/deposit-
wallets) was rejected 400 "maker address not allowed, please use the deposit
wallet flow", or -- once signature_type=3 (POLY_1271) was forced -- 400 "the
order signer address has to be the address of the API KEY". Confirmed as a
backend/SDK-auth bug (Polymarket/py-clob-client-v2#111, #70, #75), not a
config problem, and not language-specific (the same bug was reproduced
against the TS and Rust clients too).

`AsyncSecureClient.create()` derives and, if necessary, deploys the signer's
Deposit Wallet itself and issues credentials bound to it, so no manual
`signature_type`/`funder` juggling is needed. Since it's natively async,
order placement and balance reads no longer need `asyncio.to_thread`.

Orders are placed FAK (Fill-And-Kill / IOC), not FOK: a thin book partially
fills instead of the whole order being killed. Because a fill can now be
partial on *either* side, `order.size`/`shares` requested is never
trustworthy as the filled amount -- the real matched amount (maker side for
a SELL, taker side for a BUY) must always be parsed back out of the
response, same as `filled_price` is.

Position and bankroll are Execution's own truth, not cached beliefs handed
down from Strategy: `_get_bankroll`/`_get_held` are the only places real
balances are read from, each behind a short TTL cache (see
DEFAULT_BANKROLL_CACHE_SECONDS below) so converge() -- called on every
market-data tick -- doesn't hammer the real API.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING, Optional

from datastream.events import Side
from monitoring import Monitor

from .base import (
    DEFAULT_MAX_POSITION_NOTIONAL,
    DEFAULT_REJECTION_ALERT_THRESHOLD,
    DEFAULT_RETRY_COOLDOWN_SECONDS,
    ExecutionLayer,
    ExecutionResult,
    Order,
    OrderStatus,
)

if TYPE_CHECKING:
    from polymarket import AsyncSecureClient, OrderResponse

logger = logging.getLogger(__name__)

# USDC and Polymarket's conditional (outcome) tokens both use 6 decimals;
# balance-allowance responses report raw base units.
USDC_DECIMALS = 1_000_000

# converge() calls _get_bankroll()/_get_held() on every market-data tick
# (Binance klines, Polymarket book/price-change events -- many per second)
# to keep sizing honest. A short TTL cache collapses a burst of ticks into
# one real fetch without letting sizing drift far from the true balance.
DEFAULT_BANKROLL_CACHE_SECONDS = 1.5

# order.price is the book's best bid/ask at decision time, but an
# unprotected market order will happily walk a thin book to whatever price
# it takes to fill the requested amount: observed live filling a BUY modeled
# at 0.41 at 0.48 instead (a ~17% relative miss that can flip a
# modeled-positive-edge trade negative). The SDK's max_price (BUY) /
# min_price (SELL) turn the market order into a price-capped one -- the
# exchange won't match outside that range, so at worst a FAK order fills
# partially (or not at all) rather than filling at a worse price, and the
# next tick's converge() call already handles a partial fill or a
# REJECTED/FAILED result by trying again later, so this only costs a delay,
# not a silent bad fill.
DEFAULT_PRICE_PROTECTION_TOLERANCE = 0.02
MIN_TICK_PRICE = 0.01
MAX_TICK_PRICE = 0.99


class LiveExecutionLayer(ExecutionLayer):
    def __init__(
        self,
        monitor: Optional[Monitor] = None,
        retry_cooldown_seconds: float = DEFAULT_RETRY_COOLDOWN_SECONDS,
        rejection_alert_threshold: int = DEFAULT_REJECTION_ALERT_THRESHOLD,
        max_position_notional: Optional[float] = DEFAULT_MAX_POSITION_NOTIONAL,
        bankroll_cache_seconds: float = DEFAULT_BANKROLL_CACHE_SECONDS,
        price_protection_tolerance: float = DEFAULT_PRICE_PROTECTION_TOLERANCE,
    ) -> None:
        super().__init__(
            monitor=monitor,
            retry_cooldown_seconds=retry_cooldown_seconds,
            rejection_alert_threshold=rejection_alert_threshold,
            max_position_notional=max_position_notional,
        )
        self._client: Optional["AsyncSecureClient"] = None
        self._lock = asyncio.Lock()
        self._session_start_bankroll: Optional[float] = None
        self._bankroll_cache: Optional[float] = None
        self._bankroll_cache_time: float = 0.0
        self._bankroll_cache_seconds = bankroll_cache_seconds
        self._held_cache: dict = {}
        self._held_cache_time: dict = {}
        self._price_protection_tolerance = price_protection_tolerance

    async def _ensure_client(self) -> "AsyncSecureClient":
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client = await self._build_client()
            return self._client

    async def _build_client(self) -> "AsyncSecureClient":
        from polymarket import ApiKeyCreds, AsyncSecureClient, RelayerApiKey

        private_key = os.environ.get("POLYMARKET_PRIVATE_KEY")
        if not private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY environment variable is required for live execution")

        # Deposit Wallet address to trade against. None lets the SDK derive
        # (and deploy, if not already deployed) the signer's own Deposit
        # Wallet.
        wallet = os.environ.get("POLYMARKET_FUNDER_ADDRESS") or None

        relayer_key = os.environ.get("RELAYER_API_KEY")
        relayer_address = os.environ.get("RELAYER_API_KEY_ADDRESS")
        api_key = RelayerApiKey(key=relayer_key, address=relayer_address) if relayer_key and relayer_address else None

        clob_key = os.environ.get("CLOB_API_KEY")
        clob_secret = os.environ.get("CLOB_API_SECRET")
        clob_passphrase = os.environ.get("CLOB_API_PASSPHRASE")
        credentials = (
            ApiKeyCreds(apiKey=clob_key, secret=clob_secret, passphrase=clob_passphrase)
            if clob_key and clob_secret and clob_passphrase
            else None
        )

        return await AsyncSecureClient.create(
            private_key=private_key,
            wallet=wallet,
            credentials=credentials,
            api_key=api_key,
        )

    async def _get_bankroll(self) -> float:
        now = time.time()
        if self._bankroll_cache is not None and now - self._bankroll_cache_time < self._bankroll_cache_seconds:
            return self._bankroll_cache

        client = await self._ensure_client()
        bankroll = await self._fetch_collateral_balance(client)
        if self._session_start_bankroll is None:
            self._session_start_bankroll = bankroll
        self._bankroll_cache = bankroll
        self._bankroll_cache_time = now
        return bankroll

    async def _get_held(self, asset_id: str) -> float:
        now = time.time()
        cached = self._held_cache.get(asset_id)
        cached_time = self._held_cache_time.get(asset_id, 0.0)
        if cached is not None and now - cached_time < self._bankroll_cache_seconds:
            return cached

        client = await self._ensure_client()
        held = await self._fetch_conditional_balance(client, asset_id)
        self._held_cache[asset_id] = held
        self._held_cache_time[asset_id] = now
        return held

    async def status_text(self) -> str:
        try:
            bankroll = await self._get_bankroll()
        except Exception as exc:
            return f"Live execution: failed to fetch bankroll ({exc})"
        held_summary = ", ".join(
            f"{asset_id}={size:.2f}" for asset_id, size in self._held_cache.items() if size > 0
        ) or "none"
        if self._session_start_bankroll is not None:
            pnl = bankroll - self._session_start_bankroll
            return (
                f"Live execution:\nbankroll={bankroll:.2f} "
                f"session_start={self._session_start_bankroll:.2f} pnl={pnl:+.2f} held={held_summary}"
            )
        return f"Live execution:\nbankroll={bankroll:.2f} held={held_summary}"

    async def _fetch_collateral_balance(self, client: "AsyncSecureClient") -> float:
        balance = await client.get_balance_allowance(asset_type="COLLATERAL")
        return balance.balance / USDC_DECIMALS

    async def _fetch_conditional_balance(self, client: "AsyncSecureClient", asset_id: str) -> float:
        balance = await client.get_balance_allowance(asset_type="CONDITIONAL", token_id=asset_id)
        return balance.balance / USDC_DECIMALS

    async def _place_order(self, order: Order) -> ExecutionResult:
        try:
            client = await self._ensure_client()
            response = await self._place_market_order(client, order)
        except Exception as exc:
            logger.exception("Live order submission failed: %s", order)
            self._monitor.error(f"Live order submission failed: {exc}")
            return self._result(order, OrderStatus.FAILED, reason=str(exc))

        if response.ok:
            # A fill moves the real balance, so both cached figures are now
            # stale -- drop them rather than let the next tick's sizing use
            # pre-trade values.
            self._bankroll_cache = None
            self._held_cache.pop(order.asset_id, None)
            filled_price = _parse_filled_price(response, order, fallback=order.price)
            filled_size = _parse_filled_size(response, order, fallback=order.size)
            return self._result(order, OrderStatus.FILLED, filled_price=filled_price, filled_size=filled_size)

        return self._result(order, OrderStatus.REJECTED, reason=f"{response.code}: {response.message}")

    async def _place_market_order(self, client: "AsyncSecureClient", order: Order) -> "OrderResponse":
        if order.action is Side.BUY:
            max_price = _clamp_tick_price(order.price + self._price_protection_tolerance)
            return await client.place_market_order(
                token_id=order.asset_id, side="BUY", amount=order.price * order.size,
                max_price=max_price, order_type="FAK",
            )
        min_price = _clamp_tick_price(order.price - self._price_protection_tolerance)
        return await client.place_market_order(
            token_id=order.asset_id, side="SELL", shares=order.size,
            min_price=min_price, order_type="FAK",
        )


def _clamp_tick_price(price: float) -> float:
    # Rounded to whole cents so it's a valid tick-grid value under both of
    # Polymarket's tick sizes (0.01 normally, 0.001 near the 0/1 extremes --
    # a cent is a multiple of either), then clamped inside the tick grid's
    # valid range so a near-0/1 order.price plus/minus tolerance can't push
    # max_price/min_price out of (0, 1) and get rejected as invalid input.
    return round(max(MIN_TICK_PRICE, min(MAX_TICK_PRICE, price)), 2)


def _parse_filled_price(response: "OrderResponse", order: Order, fallback: float) -> float:
    # maker_amount is what we offered, taker_amount what we received: for a
    # BUY that's USDC-offered/shares-received (price = making/taking); for a
    # SELL it's shares-offered/USDC-received (price = taking/making).
    if not (response.making_amount and response.taking_amount):
        return fallback
    try:
        if order.action is Side.BUY:
            return float(response.making_amount / response.taking_amount)
        return float(response.taking_amount / response.making_amount)
    except (TypeError, ZeroDivisionError, ValueError):
        return fallback


def _parse_filled_size(response: "OrderResponse", order: Order, fallback: float) -> float:
    # Under FAK a fill can be partial on either side, so order.size (==
    # fallback) can't be trusted for a SELL either -- the actual matched
    # amount must be read back from the response same as a BUY's already is.
    # For a SELL that's the maker side (shares actually given up); for a BUY
    # it's the taker side (shares actually received, which can differ from
    # order.size's amount/estimated-price guess).
    #
    # Unlike balance-allowance (which reports raw base units, see
    # USDC_DECIMALS above), the order-placement response's making/taking
    # amounts are already human-decimal, so no /USDC_DECIMALS scaling here.
    amount = response.taking_amount if order.action is Side.BUY else response.making_amount
    if not amount:
        return fallback
    try:
        return float(amount)
    except (TypeError, ValueError):
        return fallback
