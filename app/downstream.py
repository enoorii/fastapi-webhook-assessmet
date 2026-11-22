import os

import httpx

DOWNSTREAM_URL = os.getenv(
    "DOWNSTREAM_URL",
    "http://127.0.0.1:9001/events",
)


async def forward_event(
    client: httpx.AsyncClient,
    event_id: str,
    cleaned_text: str,
    fingerprint: str,
) -> dict:
    """
    Forward a processed event using a shared HTTP client.
    """
    response = await client.post(
        DOWNSTREAM_URL,
        json={
            "event_id": event_id,
            "text": cleaned_text,
            "fingerprint": fingerprint,
        },
    )

    response.raise_for_status()
    return response.json()
