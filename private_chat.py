"""One private text thread for the people allowed on the photo page.

Pictures stay in the photo galleries. This module stores the words, who has
read them, and asks Pushover to tell the other person. A blank Pushover user
key skips that phone. A Pushover error does not discard the message.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

from private_photos import display_name, photo_allowlist

CHAT_MAX_LENGTH = 2000
CHAT_HISTORY_LIMIT = 200


def normalize_chat_body(value: Optional[str]) -> str:
    """Reject a blank or oversized message. Internal line breaks stay."""
    text = (value or "").strip()
    if not text:
        raise ValueError("Write a message first.")
    if len(text) > CHAT_MAX_LENGTH:
        raise ValueError(f"Message must be {CHAT_MAX_LENGTH} characters or fewer.")
    return text


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _iso(value: str) -> str:
    text = value.strip().replace(" ", "T")
    if text.endswith("Z") or "+" in text[10:]:
        return text
    return f"{text}Z"


def chat_title(email: str) -> str:
    """The other person's name when the thread has exactly two people."""
    viewer = email.strip().lower()
    others = sorted(addr for addr in photo_allowlist() if addr != viewer)
    if len(others) == 1:
        return display_name(others[0])
    return "Chat"


def _public_message(row: dict, *, viewer: str) -> dict:
    sender = row["sender_email"]
    return {
        "id": row["id"],
        "body": row["body"],
        "created_at": _iso(row["created_at"]),
        "sender_email": sender,
        "sender_name": display_name(sender),
        "mine": sender == viewer,
    }


def list_chat(conn: sqlite3.Connection, *, email: str) -> dict:
    """Oldest of the recent messages first, plus how many from the other person are unread."""
    viewer = email.strip().lower()
    rows = conn.execute(
        """
        SELECT * FROM (
            SELECT * FROM private_chat_messages
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        )
        ORDER BY created_at ASC, id ASC
        """,
        (CHAT_HISTORY_LIMIT,),
    ).fetchall()
    return {
        "title": chat_title(viewer),
        "unread": unread_count(conn, viewer),
        "messages": [_public_message(row, viewer=viewer) for row in rows],
    }


def unread_count(conn: sqlite3.Connection, email: str) -> int:
    """Messages from someone else since this email last opened the thread."""
    viewer = email.strip().lower()
    read = conn.execute(
        "SELECT last_read_at FROM private_chat_reads WHERE email = ?",
        (viewer,),
    ).fetchone()
    if read is None:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM private_chat_messages
            WHERE sender_email != ?
            """,
            (viewer,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM private_chat_messages
            WHERE sender_email != ? AND created_at > ?
            """,
            (viewer, read["last_read_at"]),
        ).fetchone()
    return int(row["n"] if row else 0)


def mark_chat_read(conn: sqlite3.Connection, email: str) -> int:
    """Mark the thread read as of now. Returns the unread count, which is zero."""
    viewer = email.strip().lower()
    conn.execute(
        """
        INSERT INTO private_chat_reads (email, last_read_at)
        VALUES (?, ?)
        ON CONFLICT(email) DO UPDATE SET last_read_at = excluded.last_read_at
        """,
        (viewer, _stamp()),
    )
    return 0


def post_chat_message(conn: sqlite3.Connection, *, email: str, body: str) -> dict:
    """Save one message. The caller notifies phones after the database commit."""
    viewer = email.strip().lower()
    text = normalize_chat_body(body)
    message_id = uuid.uuid4().hex
    stamp = _stamp()
    conn.execute(
        """
        INSERT INTO private_chat_messages (id, sender_email, body, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (message_id, viewer, text, stamp),
    )
    row = conn.execute(
        "SELECT * FROM private_chat_messages WHERE id = ?",
        (message_id,),
    ).fetchone()
    return _public_message(row, viewer=viewer)
