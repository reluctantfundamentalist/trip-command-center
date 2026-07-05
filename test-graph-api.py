#!/usr/bin/env python3
"""Quick standalone test of Microsoft Graph delegated OAuth + email fetching."""

import json
import os
from datetime import datetime, timezone, timedelta

import httpx

CREDS_PATH = os.path.expanduser("~/.openclaw/credentials/microsoft-graph.json")
TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def load_creds():
    with open(CREDS_PATH) as f:
        return json.load(f)


def save_creds(creds):
    with open(CREDS_PATH, "w") as f:
        json.dump(creds, f, indent=2)


async def refresh_and_test():
    creds = load_creds()
    tenant_id = creds["tenant_id"]
    refresh_token = creds.get("refresh_token")

    if not refresh_token:
        print("ERROR: No refresh_token found")
        return

    # Refresh token
    url = TOKEN_URL_TEMPLATE.format(tenant_id=tenant_id)
    payload = {
        "grant_type": "refresh_token",
        "client_id": creds["client_id"],
        "refresh_token": refresh_token,
        "scope": "https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Calendars.Read offline_access",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, data=payload)
        resp.raise_for_status()
        data = resp.json()

        access_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)

        # Save rotated creds
        creds["access_token"] = access_token
        creds["token_expires_at"] = int((datetime.now(timezone.utc) + timedelta(seconds=expires_in)).timestamp())
        if "refresh_token" in data:
            creds["refresh_token"] = data["refresh_token"]
        save_creds(creds)

        print(f"✅ Token refreshed successfully (expires in {expires_in}s)")

        # Fetch last 5 emails
        since = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        params = {
            "$select": "id,subject,from,receivedDateTime,hasAttachments",
            "$filter": f"receivedDateTime ge {since}",
            "$orderby": "receivedDateTime desc",
            "$top": "5",
        }
        email_url = f"{GRAPH_BASE}/me/messages"

        resp = await client.get(
            email_url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            params=params,
        )
        resp.raise_for_status()
        email_data = resp.json()
        messages = email_data.get("value", [])

        print(f"📧 Fetched {len(messages)} emails from last 24h:\n")
        for msg in messages:
            sender = msg.get("from", {}).get("emailAddress", {}).get("address", "unknown")
            subject = msg.get("subject", "(no subject)")
            received = msg.get("receivedDateTime", "?")
            has_att = msg.get("hasAttachments", False)
            att_marker = " 📎" if has_att else ""
            print(f"  [{received}] {sender}")
            print(f"    → {subject}{att_marker}")

        # Check delta link
        delta_link = email_data.get("@odata.deltaLink")
        if delta_link:
            print(f"\n🔄 Delta link available: {delta_link[:80]}...")
        else:
            print("\nℹ️ No delta link (expected for non-delta query)")


if __name__ == "__main__":
    import asyncio

    asyncio.run(refresh_and_test())
