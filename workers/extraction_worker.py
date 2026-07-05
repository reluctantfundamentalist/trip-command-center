"""
Extraction Worker — Kafka consumer for the extraction.jobs topic.

Consumes extraction job messages, calls Trip Claw skills
(extract_meeting_intel, extract_attachment_intel), and writes
structured output to meetings, offers, and action_items tables.
Updates the account state machine after each extraction.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from sqlalchemy import text

from config.settings import settings
from core.database import async_session_factory
from core.cache import cache
from core.state_machine import evaluate_state
from skills.extract_meeting_intel import extract_meeting_intel, MeetingIntel
from skills.extract_attachment_intel import extract_attachment_intel, AttachmentIntel

logger = logging.getLogger(__name__)

# ── Job schema ────────────────────────────────────────────────
# Each Kafka message value is JSON:
# {
#   "job_type": "email" | "attachment",
#   "raw_email_id": UUID,
#   "executive_id": UUID,
#   "airline_id": UUID,
#   "email_body": str,
#   "subject": str,
#   "thread_id": str | null,
#   "attachments": [
#       {"filename": str, "content_type": str, "extracted_text": str, "type": str}
#   ],
#   "received_at": ISO datetime str
# }


async def _assemble_account_context(airline_id: str) -> dict:
    """
    Build the account context dict needed by extraction skills.
    Checks Redis cache first, falls back to DB query.
    """
    cached = await cache.get_account_context(airline_id)
    if cached:
        return cached

    context: dict = {
        "airline_name": "",
        "recent_offers": [],
        "open_action_items": [],
        "last_meeting_summary": None,
    }

    async with async_session_factory() as session:
        # Airline name
        result = await session.execute(
            text("SELECT airline_name FROM airline_accounts WHERE id = :aid"),
            {"aid": airline_id},
        )
        row = result.scalar_one_or_none()
        if row:
            context["airline_name"] = row

        # Recent offers (last 10)
        result = await session.execute(
            text(
                "SELECT id, offer_type, status, terms, proposed_by "
                "FROM offers WHERE airline_id = :aid "
                "ORDER BY created_at DESC LIMIT 10"
            ),
            {"aid": airline_id},
        )
        context["recent_offers"] = [
            {
                "id": str(r._mapping["id"]),
                "offer_type": r._mapping["offer_type"],
                "status": r._mapping["status"],
                "terms": r._mapping["terms"],
                "proposed_by": r._mapping["proposed_by"],
            }
            for r in result.fetchall()
        ]

        # Open action items
        result = await session.execute(
            text(
                "SELECT id, description, due_date, status "
                "FROM action_items WHERE airline_id = :aid AND status IN ('open','in_progress') "
                "ORDER BY due_date ASC NULLS LAST LIMIT 20"
            ),
            {"aid": airline_id},
        )
        context["open_action_items"] = [
            {
                "id": str(r._mapping["id"]),
                "description": r._mapping["description"],
                "due_date": str(r._mapping["due_date"]) if r._mapping["due_date"] else None,
                "status": r._mapping["status"],
            }
            for r in result.fetchall()
        ]

        # Last meeting summary
        result = await session.execute(
            text(
                "SELECT summary, occurred_at FROM meetings "
                "WHERE airline_id = :aid ORDER BY occurred_at DESC LIMIT 1"
            ),
            {"aid": airline_id},
        )
        row = result.first()
        if row:
            m = row._mapping
            context["last_meeting_summary"] = {
                "summary": m["summary"],
                "occurred_at": str(m["occurred_at"]),
            }

    await cache.set_account_context(airline_id, context)
    return context


async def _get_thread_context(session, thread_id: str | None) -> list[str]:
    """Get prior thread summaries (last 5 messages)."""
    if not thread_id:
        return []

    cached = await cache.get_thread_summary(thread_id)
    if cached:
        return cached

    result = await session.execute(
        text(
            "SELECT summary FROM meetings "
            "WHERE source_email_ids && ("
            "  SELECT ARRAY_AGG(id) FROM raw_emails WHERE thread_id = :tid"
            ") "
            "ORDER BY occurred_at DESC LIMIT 5"
        ),
        {"tid": thread_id},
    )
    summaries = [r._mapping["summary"] for r in result.fetchall() if r._mapping["summary"]]
    summaries.reverse()  # chronological order

    if summaries:
        await cache.set_thread_summary(thread_id, summaries)

    return summaries


async def _write_meeting(
    session,
    airline_id: str,
    executive_id: str,
    subject: str | None,
    intel: MeetingIntel,
    raw_email_id: str,
    occurred_at: str,
) -> str:
    """Write a meeting record and return its UUID."""
    result = await session.execute(
        text(
            "INSERT INTO meetings "
            "(airline_id, executive_id, meeting_type, subject, summary, sentiment, "
            " key_contacts, occurred_at, source_email_ids) "
            "VALUES (:aid, :eid, 'email_thread', :subj, :summary, :sentiment, "
            "        :contacts, :occurred, ARRAY[:email_id]::uuid[]) "
            "RETURNING id"
        ),
        {
            "aid": airline_id,
            "eid": executive_id,
            "subj": subject,
            "summary": f"Sentiment: {intel.sentiment}. "
                       f"{len(intel.offers)} offer(s), {len(intel.action_items)} action item(s) extracted.",
            "sentiment": intel.sentiment,
            "contacts": json.dumps([asdict(c) for c in intel.key_contacts]),
            "occurred": occurred_at,
            "email_id": raw_email_id,
        },
    )
    meeting_id = str(result.scalar_one())
    return meeting_id


async def _write_offers(
    session,
    airline_id: str,
    meeting_id: str,
    raw_email_id: str,
    intel: MeetingIntel,
) -> int:
    """Write extracted offers. Returns count written."""
    count = 0
    for offer in intel.offers:
        await session.execute(
            text(
                "INSERT INTO offers "
                "(airline_id, meeting_id, offer_type, status, terms, proposed_by, "
                " proposed_at, source_email_id) "
                "VALUES (:aid, :mid, :otype, :status, :terms, :proposed_by, now(), :eid)"
            ),
            {
                "aid": airline_id,
                "mid": meeting_id,
                "otype": offer.offer_type,
                "status": offer.status,
                "terms": json.dumps(offer.terms, default=str),
                "proposed_by": offer.proposed_by,
                "eid": raw_email_id,
            },
        )
        count += 1
    return count


async def _write_action_items(
    session,
    airline_id: str,
    meeting_id: str,
    executive_id: str,
    raw_email_id: str,
    intel: MeetingIntel,
) -> int:
    """Write extracted action items. Returns count written."""
    count = 0
    for item in intel.action_items:
        await session.execute(
            text(
                "INSERT INTO action_items "
                "(airline_id, meeting_id, owner_id, description, due_date, "
                " status, source_email_id) "
                "VALUES (:aid, :mid, :oid, :desc, :due, 'open', :eid)"
            ),
            {
                "aid": airline_id,
                "mid": meeting_id,
                "oid": executive_id,  # default to the executive; can be resolved later
                "desc": item.description,
                "due": item.due_date,
                "eid": raw_email_id,
            },
        )
        count += 1
    return count


async def _mark_email_processed(session, raw_email_id: str) -> None:
    await session.execute(
        text("UPDATE raw_emails SET processed = TRUE WHERE id = :eid"),
        {"eid": raw_email_id},
    )


async def process_extraction_job(job: dict) -> dict:
    """
    Process a single extraction job.
    Returns summary stats.
    """
    job_type = job.get("job_type", "email")
    raw_email_id = job["raw_email_id"]
    executive_id = job["executive_id"]
    airline_id = job["airline_id"]
    email_body = job.get("email_body", "")
    subject = job.get("subject")
    thread_id = job.get("thread_id")
    attachments_raw = job.get("attachments", [])
    received_at_str = job.get("received_at")
    if received_at_str:
        received_at = datetime.fromisoformat(received_at_str.replace("Z", "+00:00"))
    else:
        received_at = datetime.now(timezone.utc)

    stats = {"offers": 0, "action_items": 0, "meetings": 0, "attachments_processed": 0}

    # Skip emails with no airline match — nothing to contextualize against
    if not airline_id:
        logger.info("Skipping job %s: no airline match", raw_email_id)
        return stats

    # Assemble context
    account_context = await _assemble_account_context(airline_id)

    async with async_session_factory() as session:
        # Get thread context
        thread_context = await _get_thread_context(session, thread_id)

        # Extract attachment texts
        attachment_texts = []
        for att in attachments_raw:
            att_text = att.get("extracted_text", "")
            att_type = att.get("type", "pdf")
            if att_text:
                attachment_texts.append(att_text)

                # Also run extract_attachment_intel for structured data
                try:
                    att_intel = await extract_attachment_intel(
                        attachment_text=att_text,
                        attachment_type=att_type,
                        account_context=account_context,
                    )
                    stats["attachments_processed"] += 1
                    logger.info(
                        "Attachment intel: %d excerpts from %s",
                        len(att_intel.relevant_excerpts),
                        att.get("filename", "unknown"),
                    )
                except Exception as e:
                    logger.error("Attachment extraction failed for %s: %s", att.get("filename"), e)

        # Extract meeting intelligence
        intel = await extract_meeting_intel(
            email_body=email_body,
            attachments=attachment_texts,
            account_context=account_context,
        )

        # Write meeting record
        meeting_id = await _write_meeting(
            session, airline_id, executive_id, subject, intel, raw_email_id, received_at,
        )
        stats["meetings"] = 1

        # Write offers
        stats["offers"] = await _write_offers(
            session, airline_id, meeting_id, raw_email_id, intel,
        )

        # Write action items
        stats["action_items"] = await _write_action_items(
            session, airline_id, meeting_id, executive_id, raw_email_id, intel,
        )

        # Mark email as processed
        await _mark_email_processed(session, raw_email_id)

        await session.commit()

    # Invalidate caches
    await cache.on_extraction_complete(airline_id, executive_id)

    logger.info(
        "Job %s complete: %d meeting, %d offers, %d action items, %d attachments",
        raw_email_id, stats["meetings"], stats["offers"],
        stats["action_items"], stats["attachments_processed"],
    )
    return stats


class ExtractionWorker:
    """
    Kafka consumer that processes extraction jobs from the extraction.jobs topic.
    """

    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self) -> None:
        """Initialize Kafka consumer and Redis cache, then start consuming."""
        await cache.connect()

        self._consumer = AIOKafkaConsumer(
            settings.kafka.extraction_topic,
            bootstrap_servers=settings.kafka.broker_list,
            group_id=settings.kafka.consumer_group,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=2,
            max_poll_interval_ms=900000,   # 15 minutes — LLM calls can take 30-60s
            session_timeout_ms=300000,     # 5 minutes — must be < max_poll_interval_ms
        )
        await self._consumer.start()
        self._running = True
        logger.info(
            "Extraction worker started, consuming from %s (group: %s)",
            settings.kafka.extraction_topic,
            settings.kafka.consumer_group,
        )

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        await cache.close()
        logger.info("Extraction worker stopped")

    async def run(self) -> None:
        """Main consumption loop."""
        await self.start()

        try:
            while self._running:
                async for msg in self._consumer:
                    if not self._running:
                        break

                    try:
                        job = msg.value
                        logger.info(
                            "Processing job: email=%s airline=%s",
                            job.get("raw_email_id", "?"),
                            job.get("airline_id", "?"),
                        )
                        await process_extraction_job(job)
                        await self._consumer.commit()
                    except Exception as e:
                        logger.error(
                            "Job processing failed (partition=%d offset=%d): %s",
                            msg.partition, msg.offset, e,
                            exc_info=True,
                        )
                        # Commit anyway to avoid poison pill blocking the queue.
                        # Dead-letter queue handling would go here in production.
                        await self._consumer.commit()
        finally:
            await self.stop()


async def main() -> None:
    """Entry point for running the extraction worker."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = ExtractionWorker()
    loop = asyncio.get_event_loop()

    # Graceful shutdown on SIGTERM/SIGINT
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
