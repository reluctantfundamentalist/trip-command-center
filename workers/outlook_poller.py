"""
Outlook Delta Poller — Microsoft Graph API email ingestion for Trip.com Flights Sales Intelligence.

Polls each executive's Outlook inbox every 10 minutes using the Microsoft Graph delta API,
downloads new emails and attachments, matches them to airline accounts by domain,
writes to raw_emails table, and publishes extraction jobs to Kafka.

Usage:
    python -m workers.outlook_poller

Environment:
    OUTLOOK_TENANT_ID
    OUTLOOK_CLIENT_ID
    OUTLOOK_CLIENT_SECRET
    KAFKA_BROKERS
    KAFKA_EXTRACTION_TOPIC
    DATABASE_URL
    INGESTION_POLL_INTERVAL_SECONDS (default 600)
    INGESTION_BACKFILL_DAYS (default 90)
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
from datetime import datetime, timezone, timedelta
from typing import Any
from uuid import UUID

import httpx
from aiokafka import AIOKafkaProducer
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from core.database import async_session_factory, engine
from core.attachment_parser import extract_text_from_attachment

logger = logging.getLogger(__name__)

# Microsoft Graph endpoints
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

# Path to existing delegated OAuth credentials
CREDS_PATH = os.path.expanduser("~/.openclaw/credentials/microsoft-graph.json")


class GraphAuthDelegated:
    """Handles Microsoft Graph delegated OAuth (refresh_token) for testing with Anuj's account."""

    def __init__(self, creds_path: str = CREDS_PATH) -> None:
        self._creds_path = creds_path
        self._creds: dict = {}
        self._token: str | None = None
        self._token_expires_at: datetime | None = None
        self._http: httpx.AsyncClient = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._http.aclose()

    def _load_creds(self) -> dict:
        with open(self._creds_path) as f:
            return json.load(f)

    def _save_creds(self, creds: dict) -> None:
        with open(self._creds_path, "w") as f:
            json.dump(creds, f, indent=2)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=15),
        reraise=True,
    )
    async def get_token(self) -> str:
        """Return a valid access token, refreshing via refresh_token if needed."""
        if self._token and self._token_expires_at and datetime.now(timezone.utc) < self._token_expires_at:
            return self._token

        creds = self._load_creds()
        tenant_id = creds["tenant_id"]
        refresh_token = creds.get("refresh_token")

        if not refresh_token:
            raise RuntimeError("No refresh_token found in credentials file")

        url = TOKEN_URL_TEMPLATE.format(tenant_id=tenant_id)
        payload = {
            "grant_type": "refresh_token",
            "client_id": creds["client_id"],
            "refresh_token": refresh_token,
            "scope": "https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Calendars.Read offline_access",
        }

        resp = await self._http.post(url, data=payload)
        resp.raise_for_status()
        data = resp.json()

        self._token = data["access_token"]
        expires_in = data.get("expires_in", 3600)
        self._token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 300)

        # Update and save creds (refresh_token may rotate)
        creds["access_token"] = self._token
        creds["token_expires_at"] = int(self._token_expires_at.timestamp())
        if "refresh_token" in data:
            creds["refresh_token"] = data["refresh_token"]
        self._save_creds(creds)

        logger.info("Graph delegated token refreshed, expires in %ds", expires_in)
        return self._token

    def get_delta_token(self) -> str | None:
        """Return the stored delta token from the creds file."""
        creds = self._load_creds()
        return creds.get("delta_token") or None

    def save_delta_token(self, delta_token: str) -> None:
        """Persist the delta token back to the creds file."""
        creds = self._load_creds()
        creds["delta_token"] = delta_token
        self._save_creds(creds)


class OutlookPoller:
    """Polls Microsoft Graph delta API for new emails."""

    def __init__(self) -> None:
        self._auth = GraphAuthDelegated()
        self._producer: AIOKafkaProducer | None = None
        self._running = False
        self._poll_interval = settings.ingestion.poll_interval_seconds
        self._backfill_days = settings.ingestion.backfill_days
        self._excluded_senders = settings.ingestion.excluded_senders

    async def _ensure_producer(self) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka.broker_list,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            await self._producer.start()
            logger.info("Kafka producer started")

    async def _stop_producer(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def _graph_get(self, url: str) -> dict:
        """Make an authenticated GET request to Microsoft Graph."""
        token = await self._auth.get_token()
        resp = await self._auth._http.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def _find_anuj_executive(self) -> dict | None:
        """Find Anuj's executive record in the DB."""
        async with async_session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT id, employee_id, email, full_name, outlook_delta_token "
                    "FROM executives WHERE email = 'anuj.bansal@trip.com' LIMIT 1"
                )
            )
            row = result.first()
            if row:
                return dict(row._mapping)
            return None

    async def _update_delta_token(self, exec_id: str, delta_token: str) -> None:
        """Persist the new delta token for an executive."""
        async with async_session_factory() as session:
            await session.execute(
                text(
                    "UPDATE executives SET outlook_delta_token = :token WHERE id = :eid"
                ),
                {"token": delta_token, "eid": exec_id},
            )
            await session.commit()
            logger.debug("Updated delta token for exec %s", exec_id)

    async def _match_airline_by_domain(self, email_address: str) -> str | None:
        """Match an email address to an airline account by domain."""
        domain = email_address.split("@")[-1].lower() if "@" in email_address else ""
        if not domain:
            return None

        async with async_session_factory() as session:
            # Try exact domain match on email_domains array
            result = await session.execute(
                text(
                    "SELECT id FROM airline_accounts WHERE :domain = ANY(email_domains) LIMIT 1"
                ),
                {"domain": domain},
            )
            row = result.scalar_one_or_none()
            if row:
                return str(row)

            # Try partial match (e.g., subdomain)
            result = await session.execute(
                text(
                    "SELECT id FROM airline_accounts WHERE EXISTS ("
                    "  SELECT 1 FROM unnest(email_domains) AS d WHERE :domain LIKE '%' || d"
                    ") LIMIT 1"
                ),
                {"domain": domain},
            )
            row = result.scalar_one_or_none()
            if row:
                return str(row)

        return None

    def _should_exclude(self, sender: str) -> bool:
        """Check if sender matches excluded patterns."""
        sender_lower = sender.lower()
        for pattern in self._excluded_senders:
            if pattern.endswith("*"):
                if sender_lower.startswith(pattern[:-1].lower()):
                    return True
            elif pattern.lower() in sender_lower:
                return True
        return False

    async def _fetch_message(self, message_id: str) -> dict:
        """Fetch full message details including body."""
        url = f"{GRAPH_BASE}/me/messages/{message_id}"
        url += "?$select=id,subject,body,from,toRecipients,receivedDateTime,conversationId,hasAttachments"
        return await self._graph_get(url)

    async def _fetch_attachments(self, message_id: str) -> list[dict]:
        """Fetch and download attachments for a message."""
        url = f"{GRAPH_BASE}/me/messages/{message_id}/attachments"
        data = await self._graph_get(url)
        attachments = data.get("value", [])

        parsed_attachments: list[dict] = []
        for att in attachments:
            att_type = att.get("@odata.type", "")
            if att_type == "#microsoft.graph.fileAttachment":
                content_bytes = att.get("contentBytes")
                if content_bytes:
                    file_bytes = base64.b64decode(content_bytes)
                    att_type_str, extracted_text = extract_text_from_attachment(
                        file_bytes,
                        att.get("name", "unknown"),
                        att.get("contentType"),
                    )
                    parsed_attachments.append({
                        "filename": att.get("name", "unknown"),
                        "content_type": att.get("contentType", "application/octet-stream"),
                        "size": len(file_bytes),
                        "type": att_type_str,
                        "extracted_text": extracted_text,
                    })
            elif att_type == "#microsoft.graph.itemAttachment":
                # Embedded items (e.g., contact cards) — skip for now
                logger.debug("Skipping item attachment: %s", att.get("name"))

        return parsed_attachments

    async def _insert_raw_email(
        self,
        executive_id: str,
        outlook_message_id: str,
        airline_id: str | None,
        thread_id: str | None,
        from_address: str,
        to_addresses: list[str],
        subject: str | None,
        body_text: str | None,
        received_at: datetime,
        attachments: list[dict],
    ) -> str:
        """Insert a raw email into the database and return its UUID."""
        async with async_session_factory() as session:
            result = await session.execute(
                text(
                    "INSERT INTO raw_emails "
                    "(outlook_message_id, executive_id, airline_id, thread_id, "
                    "from_address, to_addresses, subject, body_text, received_at, attachments) "
                    "VALUES (:omid, :eid, :aid, :tid, :from, :to, :subj, :body, :received, :att) "
                    "RETURNING id"
                ),
                {
                    "omid": outlook_message_id,
                    "eid": executive_id,
                    "aid": airline_id,
                    "tid": thread_id,
                    "from": from_address,
                    "to": to_addresses,
                    "subj": subject,
                    "body": body_text,
                    "received": received_at,
                    "att": json.dumps(attachments),
                },
            )
            raw_email_id = str(result.scalar_one())
            await session.commit()
            return raw_email_id

    async def _publish_extraction_job(self, job: dict) -> None:
        """Publish an extraction job to Kafka."""
        await self._ensure_producer()
        key = job.get("executive_id", "")
        await self._producer.send(
            settings.kafka.extraction_topic,
            key=key,
            value=job,
        )
        logger.info(
            "Published extraction job: email=%s airline=%s exec=%s",
            job.get("raw_email_id"), job.get("airline_id"), job.get("executive_id"),
        )

    async def _poll(self) -> int:
        """Poll Anuj's inbox. Returns number of new emails processed."""
        executive = await self._find_anuj_executive()
        if not executive:
            logger.warning("Anuj's executive record not found in database. Seed executives first.")
            return 0

        exec_id = str(executive["id"])
        delta_token = self._auth.get_delta_token()

        processed = 0

        # Build delta URL
        if delta_token:
            url = delta_token
        else:
            # Initial sync: get last N days
            since = datetime.now(timezone.utc) - timedelta(days=self._backfill_days)
            since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            url = (
                f"{GRAPH_BASE}/me/mailFolders/inbox/messages/delta"
                f"?$filter=receivedDateTime ge {since_str}"
                f"&$select=id,subject,from,toRecipients,receivedDateTime,conversationId,hasAttachments"
                f"&$top=50"
            )

        while url:
            try:
                data = await self._graph_get(url)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 410:
                    # Delta token expired — reset and start over
                    logger.warning("Delta token expired, resetting")
                    self._auth.save_delta_token("")
                    return 0
                raise

            messages = data.get("value", [])
            logger.info("Fetched %d messages", len(messages))

            for msg in messages:
                # Skip deleted messages (delta API sends them with @removed)
                if "@removed" in msg:
                    continue

                msg_id = msg.get("id")
                subject = msg.get("subject")
                from_addr = msg.get("from", {}).get("emailAddress", {}).get("address", "")
                conversation_id = msg.get("conversationId")
                received_str = msg.get("receivedDateTime")
                has_attachments = msg.get("hasAttachments", False)

                if not msg_id or not from_addr:
                    continue

                if self._should_exclude(from_addr):
                    logger.debug("Skipping excluded sender: %s", from_addr)
                    continue

                # Check if already ingested
                async with async_session_factory() as session:
                    existing = await session.execute(
                        text("SELECT 1 FROM raw_emails WHERE outlook_message_id = :omid"),
                        {"omid": msg_id},
                    )
                    if existing.scalar_one_or_none():
                        logger.debug("Already ingested: %s", msg_id)
                        continue

                # Fetch full message body
                try:
                    full_msg = await self._fetch_message(msg_id)
                    body = full_msg.get("body", {})
                    body_text = body.get("content", "") if body.get("contentType") == "text" else None
                    if not body_text:
                        body_text = body.get("content", "")
                except Exception as e:
                    logger.error("Failed to fetch message body %s: %s", msg_id, e)
                    body_text = ""

                # Fetch attachments
                attachments: list[dict] = []
                if has_attachments:
                    try:
                        attachments = await self._fetch_attachments(msg_id)
                    except Exception as e:
                        logger.error("Failed to fetch attachments for %s: %s", msg_id, e)

                # Match airline
                to_addrs = [r.get("emailAddress", {}).get("address", "") for r in msg.get("toRecipients", [])]
                airline_id = await self._match_airline_by_domain(from_addr)
                if not airline_id:
                    for addr in to_addrs:
                        airline_id = await self._match_airline_by_domain(addr)
                        if airline_id:
                            break

                # Parse received timestamp
                received_at = datetime.fromisoformat(received_str.replace("Z", "+00:00")) if received_str else datetime.now(timezone.utc)

                # Insert raw email
                try:
                    # Strip null bytes which PostgreSQL cannot handle
                    clean_body = (body_text or "").replace("\x00", "")
                    raw_email_id = await self._insert_raw_email(
                        executive_id=exec_id,
                        outlook_message_id=msg_id,
                        airline_id=airline_id,
                        thread_id=conversation_id,
                        from_address=from_addr,
                        to_addresses=[a for a in to_addrs if a],
                        subject=subject,
                        body_text=clean_body,
                        received_at=received_at,
                        attachments=attachments,
                    )
                except Exception as e:
                    logger.error("Failed to insert raw email %s: %s", msg_id, e)
                    continue

                # Publish to Kafka for extraction
                job = {
                    "job_type": "email",
                    "raw_email_id": raw_email_id,
                    "executive_id": exec_id,
                    "airline_id": airline_id,
                    "email_body": body_text or "",
                    "subject": subject,
                    "thread_id": conversation_id,
                    "attachments": attachments,
                    "received_at": received_at.isoformat(),
                }
                try:
                    await self._publish_extraction_job(job)
                except Exception as e:
                    logger.error("Failed to publish job for %s: %s", raw_email_id, e)

                processed += 1

            # Pagination / delta link
            next_link = data.get("@odata.nextLink")
            delta_link = data.get("@odata.deltaLink")

            if next_link:
                url = next_link
            elif delta_link:
                # Store delta token for next poll
                self._auth.save_delta_token(delta_link)
                await self._update_delta_token(exec_id, delta_link)
                url = None
            else:
                url = None

        return processed

    async def run_once(self) -> dict:
        """Run a single polling cycle."""
        count = await self._poll()
        logger.info("Poll cycle complete: %d emails processed", count)
        return {"processed": count, "executives": 1}

    async def run_forever(self) -> None:
        """Run polling cycles indefinitely."""
        self._running = True
        logger.info(
            "Outlook poller started (interval: %ds, backfill: %dd)",
            self._poll_interval, self._backfill_days,
        )

        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Poll cycle error: %s", e, exc_info=True)

            # Sleep with interruptibility
            try:
                await asyncio.wait_for(
                    asyncio.Event().wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                continue

        logger.info("Outlook poller stopped")

    def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        await self._stop_producer()
        await self._auth.close()


async def main() -> None:
    """Entry point for the Outlook delta poller."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    poller = OutlookPoller()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, poller.stop)

    try:
        await poller.run_forever()
    finally:
        await poller.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
