"""
WhatsApp Bridge — for Chief of Staff (Dynamic Grok Bot Desk)
==============================================================
"""

import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route
from twilio.rest import Client
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

TWILIO_ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_WHATSAPP_FROM = os.environ["TWILIO_WHATSAPP_FROM"]
HUMAN_WHATSAPP_TO = os.environ["HUMAN_WHATSAPP_TO"]
DB_PATH = os.environ.get("WHATSAPP_BRIDGE_DB", "whatsapp_bridge.sqlite3")
PORT = int(os.environ.get("PORT", "10000"))

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
_db_lock = threading.Lock()


def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inbound_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            body TEXT NOT NULL,
            received_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _store_message(sender: str, body: str, received_at: str) -> None:
    with _db_lock:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO inbound_messages (sender, body, received_at) VALUES (?, ?, ?)",
                (sender, body, received_at),
            )
            conn.commit()
        finally:
            conn.close()


def _drain_messages() -> list[dict]:
    with _db_lock:
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT id, sender, body, received_at FROM inbound_messages ORDER BY id ASC"
            ).fetchall()
            if rows:
                ids = [r[0] for r in rows]
                conn.executemany(
                    "DELETE FROM inbound_messages WHERE id = ?", [(i,) for i in ids]
                )
                conn.commit()
            return [{"from": r[1], "body": r[2], "received_at": r[3]} for r in rows]
        finally:
            conn.close()


_get_conn().close()

mcp = FastMCP(
    "whatsapp-bridge",
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)
mcp.settings.streamable_http_path = "/"


@mcp.tool()
def send_whatsapp_message(text: str) -> dict:
    """Send a WhatsApp message to the desk's configured human number."""
    message = twilio_client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=HUMAN_WHATSAPP_TO,
        body=text,
    )
    return {"status": "sent", "sid": message.sid}


@mcp.tool()
def get_new_whatsapp_messages() -> list[dict]:
    """Return every WhatsApp message received since the last call."""
    return _drain_messages()


async def whatsapp_webhook(request: Request):
    form = await request.form()
    body = str(form.get("Body", "")).strip()
    sender = str(form.get("From", ""))
    _store_message(sender, body, datetime.now(timezone.utc).isoformat())
    return PlainTextResponse("", status_code=204)


@asynccontextmanager
async def lifespan(app):
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[
        Route("/whatsapp-webhook", endpoint=whatsapp_webhook, methods=["POST"]),
        Mount("/mcp-server", app=mcp.streamable_http_app()),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
