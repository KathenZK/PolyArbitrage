"""Minimal auto-redeem worker for resolved Polymarket positions.

This worker intentionally does one thing only:
  - poll the official positions API for `redeemable=true`
  - restrict to conditionIds touched by this strategy
  - submit a gasless PROXY redeem transaction through Polymarket relayer
  - reconcile relayer transaction state

It does not try to auto-sell before resolution.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import keccak, to_bytes, to_checksum_address
from hexbytes import HexBytes

from src.data.polymarket_client import PolymarketCLOBClient, PolymarketGammaClient
from src.output.db import (
    get_pending_redeems,
    get_tracked_live_condition_ids,
    update_redeem,
    upsert_redeem_candidate,
)

logger = logging.getLogger(__name__)

RELAYER_URL = "https://relayer-v2.polymarket.com"
POLYGON_CHAIN_ID = 137
PROXY_FACTORY = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
RELAY_HUB = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
PROXY_INIT_CODE_HASH = "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
DEFAULT_COLLATERAL = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
DEFAULT_GAS_LIMIT = 10_000_000
PROXY_CALL_TYPE = 1
FINAL_STATES = {"STATE_MINED", "STATE_CONFIRMED"}
FAIL_STATES = {"STATE_FAILED", "STATE_INVALID"}


@dataclass
class RedeemStatus:
    enabled: bool
    armed: bool
    owner: str
    funder: str
    reason: str = ""


class ProxyRedeemer:
    def __init__(
        self,
        gamma: PolymarketGammaClient,
        *,
        enabled: bool = True,
        poll_interval_secs: float = 180.0,
        tracked_strategy_only: bool = True,
        require_auth: bool = True,
    ):
        self._gamma = gamma
        self._enabled = enabled
        self._poll_interval = poll_interval_secs
        self._tracked_strategy_only = tracked_strategy_only
        self._require_auth = require_auth
        self._db = None
        self._clob: PolymarketCLOBClient | None = None
        self._collateral_address = ""

        self._private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        self._funder = os.getenv("POLYMARKET_FUNDER", "").strip()
        self._relayer_api_key = os.getenv("RELAYER_API_KEY", "").strip()
        self._relayer_api_key_address = os.getenv("RELAYER_API_KEY_ADDRESS", "").strip()
        self._owner = Account.from_key(self._private_key).address if self._private_key else ""
        self._last_run = 0.0
        self._warned_disabled = False

    @property
    def poll_interval(self) -> float:
        return self._poll_interval

    def attach_db(self, conn):
        self._db = conn

    def attach_clob(self, clob: PolymarketCLOBClient | None):
        self._clob = clob

    def status(self) -> RedeemStatus:
        if not self._enabled:
            return RedeemStatus(False, False, self._owner, self._funder, "disabled in config")
        if not self._private_key or not self._owner:
            return RedeemStatus(True, False, self._owner, self._funder, "POLYMARKET_PRIVATE_KEY missing")
        if not self._funder:
            return RedeemStatus(True, False, self._owner, self._funder, "POLYMARKET_FUNDER missing")
        derived_proxy = self._derive_proxy_wallet(self._owner)
        if derived_proxy.lower() != self._funder.lower():
            return RedeemStatus(
                True,
                False,
                self._owner,
                self._funder,
                f"derived proxy {derived_proxy} does not match funder",
            )
        if self._require_auth and (not self._relayer_api_key or not self._relayer_api_key_address):
            return RedeemStatus(True, False, self._owner, self._funder, "RELAYER_API_KEY not configured")
        return RedeemStatus(True, True, self._owner, self._funder, "")

    async def run_once(self) -> int:
        status = self.status()
        if not status.armed:
            if status.enabled and not self._warned_disabled:
                logger.warning(f"Redeem worker disabled: {status.reason}")
                self._warned_disabled = True
            return 0
        if self._db is None:
            return 0

        tracked = get_tracked_live_condition_ids(self._db) if self._tracked_strategy_only else set()
        positions = await self._gamma.get_positions(self._funder, redeemable=True, limit=500)
        discovered = 0
        for position in positions:
            condition_id = str(position.get("conditionId", "") or "")
            if not condition_id:
                continue
            if tracked and condition_id not in tracked:
                continue
            size = float(position.get("size", 0) or 0)
            if size <= 0:
                continue
            upsert_redeem_candidate(
                self._db,
                condition_id=condition_id,
                asset=str(position.get("title", "") or position.get("asset", "")),
                market_slug=str(position.get("slug", "") or position.get("eventSlug", "")),
                proxy_wallet=str(position.get("proxyWallet", "") or self._funder),
                outcome=str(position.get("outcome", "") or ""),
                size=size,
                raw_data=position,
            )
            discovered += 1

        submitted = 0
        for row in get_pending_redeems(self._db):
            redeem_id = int(row["id"])
            status_value = str(row["status"] or "")
            if status_value in {"redeemable", "retry"}:
                try:
                    response = await self._submit_redeem(str(row["condition_id"]))
                    update_redeem(
                        self._db,
                        redeem_id,
                        status="submitted",
                        transaction_id=str(response.get("transactionID", "") or ""),
                        transaction_hash=str(response.get("transactionHash", "") or response.get("hash", "") or ""),
                        last_error="",
                        raw_data=response,
                    )
                    submitted += 1
                    logger.info(f"Redeem submitted for {row['condition_id']}: {response.get('transactionID')}")
                except Exception as exc:
                    update_redeem(self._db, redeem_id, status="retry", last_error=str(exc))
                    logger.warning(f"Redeem submit failed for {row['condition_id']}: {exc}")
            elif status_value == "submitted":
                txid = str(row.get("transaction_id", "") or "")
                if not txid:
                    update_redeem(self._db, redeem_id, status="retry", last_error="missing transaction id")
                    continue
                try:
                    transactions = await self._relayer_request(
                        "GET",
                        "/transaction",
                        params={"id": txid},
                        authed=True,
                    )
                    if not transactions:
                        continue
                    txn = transactions[0]
                    txn_state = str(txn.get("state", "") or "")
                    if txn_state in FINAL_STATES:
                        update_redeem(
                            self._db,
                            redeem_id,
                            status="confirmed",
                            transaction_hash=str(txn.get("transactionHash", "") or ""),
                            raw_data=txn,
                        )
                        logger.info(f"Redeem confirmed for {row['condition_id']}: {txid}")
                    elif txn_state in FAIL_STATES:
                        update_redeem(
                            self._db,
                            redeem_id,
                            status="retry",
                            last_error=f"relayer state {txn_state}",
                            raw_data=txn,
                        )
                except Exception as exc:
                    logger.debug(f"Redeem transaction fetch failed for {txid}: {exc}")

        self._last_run = time.time()
        return discovered + submitted

    async def _submit_redeem(self, condition_id: str) -> dict[str, Any]:
        relay_payload = await self._relayer_request(
            "GET",
            "/relay-payload",
            params={"address": self._owner, "type": "PROXY"},
            authed=False,
        )
        relay_address = str(relay_payload.get("address", "") or "")
        nonce = str(relay_payload.get("nonce", "") or "0")
        if not relay_address:
            raise RuntimeError("relayer payload missing relay address")

        collateral = await self._get_collateral_address()
        redeem_call = self._encode_redeem_positions(collateral, condition_id)
        proxy_call = self._encode_proxy_calls(
            [
                {
                    "typeCode": PROXY_CALL_TYPE,
                    "to": CONDITIONAL_TOKENS,
                    "value": 0,
                    "data": redeem_call,
                }
            ]
        )
        tx_hash = self._create_proxy_struct_hash(
            from_address=self._owner,
            to_address=PROXY_FACTORY,
            data_hex=proxy_call,
            tx_fee="0",
            gas_price="0",
            gas_limit=str(DEFAULT_GAS_LIMIT),
            nonce=nonce,
            relay_hub=RELAY_HUB,
            relay_address=relay_address,
        )
        signature = self._sign_hash(tx_hash)
        body = {
            "from": self._owner,
            "to": PROXY_FACTORY,
            "proxyWallet": self._derive_proxy_wallet(self._owner),
            "data": proxy_call,
            "nonce": nonce,
            "signature": signature,
            "signatureParams": {
                "gasPrice": "0",
                "gasLimit": str(DEFAULT_GAS_LIMIT),
                "relayerFee": "0",
                "relayHub": RELAY_HUB,
                "relay": relay_address,
            },
            "type": "PROXY",
            "metadata": json.dumps({"kind": "redeem", "conditionId": condition_id}),
        }
        return await self._relayer_request("POST", "/submit", json_body=body, authed=True)

    async def _relayer_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        authed: bool,
    ) -> Any:
        session = await self._gamma._ensure_session()
        headers = {}
        if authed:
            if not self._relayer_api_key or not self._relayer_api_key_address:
                raise RuntimeError("RELAYER_API_KEY and RELAYER_API_KEY_ADDRESS are required")
            headers["RELAYER_API_KEY"] = self._relayer_api_key
            headers["RELAYER_API_KEY_ADDRESS"] = self._relayer_api_key_address
        async with session.request(
            method,
            f"{RELAYER_URL}{path}",
            params=params,
            json=json_body,
            headers=headers,
        ) as response:
            text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"relayer {path} failed ({response.status}): {text[:400]}")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text

    async def _get_collateral_address(self) -> str:
        if self._collateral_address:
            return self._collateral_address
        try:
            clob = self._clob or self._build_clob()
            self._collateral_address = clob.get_collateral_address()
        except Exception as exc:
            logger.debug(f"Collateral address lookup failed, using default: {exc}")
            self._collateral_address = DEFAULT_COLLATERAL
        return self._collateral_address

    def _build_clob(self) -> PolymarketCLOBClient:
        signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0").strip() or "0")
        clob = PolymarketCLOBClient(
            self._private_key,
            signature_type=signature_type,
            funder=self._funder,
            api_key=os.getenv("POLYMARKET_API_KEY", "").strip(),
            api_secret=os.getenv("POLYMARKET_API_SECRET", "").strip(),
            api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE", "").strip(),
        )
        self._clob = clob
        return clob

    @staticmethod
    def _derive_proxy_wallet(owner_address: str) -> str:
        salt = keccak(to_bytes(hexstr=owner_address))
        payload = (
            b"\xff"
            + to_bytes(hexstr=PROXY_FACTORY)
            + salt
            + to_bytes(hexstr=PROXY_INIT_CODE_HASH)
        )
        return to_checksum_address(keccak(payload)[-20:])

    @staticmethod
    def _sign_hash(hash_hex: str) -> str:
        signature = Account.sign_message(
            encode_defunct(HexBytes(hash_hex)),
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", "").strip(),
        ).signature.hex()
        return signature if signature.startswith("0x") else f"0x{signature}"

    @staticmethod
    def _create_proxy_struct_hash(
        *,
        from_address: str,
        to_address: str,
        data_hex: str,
        tx_fee: str,
        gas_price: str,
        gas_limit: str,
        nonce: str,
        relay_hub: str,
        relay_address: str,
    ) -> str:
        payload = (
            b"rlx:"
            + to_bytes(hexstr=from_address)
            + to_bytes(hexstr=to_address)
            + to_bytes(hexstr=data_hex)
            + int(tx_fee).to_bytes(32, "big")
            + int(gas_price).to_bytes(32, "big")
            + int(gas_limit).to_bytes(32, "big")
            + int(nonce).to_bytes(32, "big")
            + to_bytes(hexstr=relay_hub)
            + to_bytes(hexstr=relay_address)
        )
        digest = HexBytes(keccak(payload)).hex()
        return digest if digest.startswith("0x") else f"0x{digest}"

    @staticmethod
    def _encode_proxy_calls(calls: list[dict[str, Any]]) -> str:
        selector = keccak(text="proxy((uint8,address,uint256,bytes)[])")[:4]
        tuples = [
            (
                int(call["typeCode"]),
                to_checksum_address(call["to"]),
                int(call.get("value", 0)),
                to_bytes(hexstr=str(call["data"])),
            )
            for call in calls
        ]
        encoded = encode(["(uint8,address,uint256,bytes)[]"], [tuples])
        data = HexBytes(selector + encoded).hex()
        return data if data.startswith("0x") else f"0x{data}"

    @staticmethod
    def _encode_redeem_positions(collateral_token: str, condition_id: str) -> str:
        selector = keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
        encoded = encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [
                to_checksum_address(collateral_token),
                b"\x00" * 32,
                to_bytes(hexstr=condition_id),
                [1, 2],
            ],
        )
        data = HexBytes(selector + encoded).hex()
        return data if data.startswith("0x") else f"0x{data}"
