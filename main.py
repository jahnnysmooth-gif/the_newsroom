import asyncio
import os
from datetime import datetime, UTC
from typing import List, Optional

import discord

from player_matcher import match_players
from event_classifier import classify_event
from story_engine import StoryEngine
from blurb_generator import generate_blurb

TOKEN = os.getenv("DISCORD_TOKEN")

# Hidden input channels
CHANNEL_IDS = [
    1482543088223391785,  # beat-writers
    1480361783238852618,  # closer-news
]

# Test output channel
OUTPUT_CHANNEL_ID = 1481473655446704278

SCAN_INTERVAL = 15  # seconds

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

last_checked = datetime.now(UTC)
story_engine = StoryEngine()


def extract_embed_text(message: discord.Message) -> str:
    parts: List[str] = []

    if message.content:
        parts.append(message.content)

    for embed in message.embeds:
        if embed.title:
            parts.append(embed.title)
        if embed.description:
            parts.append(embed.description)

        for field in embed.fields:
            if field.name:
                parts.append(field.name)
            if field.value:
                parts.append(field.value)

    return "\n".join(part.strip() for part in parts if part and str(part).strip())


def extract_embed_url(message: discord.Message) -> Optional[str]:
    for embed in message.embeds:
        if embed.url:
            return embed.url
    return None


def extract_source_name(message: discord.Message) -> str:
    for embed in message.embeds:
        if embed.author and embed.author.name:
            return embed.author.name
    return str(message.author)


def extract_source_type(channel_name: str) -> str:
    name = (channel_name or "").lower()

    if "beat" in name:
        return "beat_writer"
    if "national" in name:
        return "national_reporter"
    if "closer" in name or "bullpen" in name:
        return "bullpen_feed"
    if "injur" in name:
        return "injury_feed"
    if "lineup" in name:
        return "lineup_feed"
    if "transaction" in name:
        return "transaction_feed"

    return "unknown"


async def send_to_output(text: str) -> None:
    channel = client.get_channel(OUTPUT_CHANNEL_ID)

    if not channel:
        print(f"[ERROR] Output channel not found or no access: {OUTPUT_CHANNEL_ID}")
        return

    try:
        await channel.send(text)
    except Exception as e:
        print(f"[ERROR] Failed to send message to output channel: {e}")


async def process_message(message: discord.Message) -> None:
    raw_text = extract_embed_text(message)
    if not raw_text:
        return

    print(f"\n[PROCESSING] message_id={message.id}")
    print(f"[RAW TEXT]\n{raw_text}")

    players = match_players(raw_text)
    if not players:
        print("[NO PLAYER MATCHES]")
        return

    classification = classify_event(raw_text)
    print(f"[CLASSIFICATION] {classification.to_dict()}")

    source_name = extract_source_name(message)
    source_type = extract_source_type(message.channel.name)
    source_url = extract_embed_url(message)

    for player in players:
        player_name = player["name"]

        story = story_engine.ingest(
            player_name=player_name,
            event_type=classification.event_type,
            raw_text=raw_text,
            source=source_name,
            source_type=source_type,
            timestamp=message.created_at,
            team=None,  # team support comes later
            confidence=classification.confidence,
            priority=classification.priority,
            matched_keywords=classification.matched_keywords,
            is_flash_candidate=classification.is_flash_candidate,
            needs_followup=classification.needs_followup,
            url=source_url,
        )

        print(
            f"[STORY] id={story.story_id} | player={story.player_name} | "
            f"event={story.event_type} | items={len(story.items)}"
        )


async def run_publish_checks() -> None:
    now = datetime.now(UTC)

    # Flash alerts
    for story in story_engine.get_flash_ready_stories(now):
        flash_text = f"🚨 **FLASH ALERT** 🚨\n**{story.player_name}** — {story.event_type}"
        await send_to_output(flash_text)
        print(f"[FLASH SENT] {story.story_id}")
        story_engine.mark_flash_posted(story.story_id, now)

    # Full blurbs
    for story in story_engine.get_blurb_ready_stories(now):
        blurb = generate_blurb(story)
        await send_to_output(blurb)
        print(f"[BLURB SENT] {story.story_id}")
        story_engine.mark_blurb_posted(story.story_id)

    # Follow-up blurbs
    for story in story_engine.get_followup_ready_stories(now):
        blurb = generate_blurb(story)
        await send_to_output(f"🔁 **UPDATE**\n\n{blurb}")
        print(f"[FOLLOW-UP SENT] {story.story_id}")
        story_engine.mark_followup_posted(story.story_id)

    # Close expired stories
    closed = story_engine.close_expired_stories(now)
    for story_id in closed:
        print(f"[CLOSED STORY] {story_id}")


@client.event
async def on_ready():
    print(f"[NEWSROOM] Logged in as {client.user} (id={client.user.id})")
    asyncio.create_task(batch_loop())


async def batch_loop():
    global last_checked

    await client.wait_until_ready()

    while not client.is_closed():
        now = datetime.now(UTC)
        print(f"\n[SCAN] {now.isoformat()}")

        newest_seen = last_checked

        for channel_id in CHANNEL_IDS:
            channel = client.get_channel(channel_id)

            if not channel:
                print(f"[ERROR] Channel not found or no access: {channel_id}")
                continue

            async for message in channel.history(limit=50):
                if message.created_at <= last_checked:
                    break

                if message.created_at > newest_seen:
                    newest_seen = message.created_at

                print(f"\n[NEW MESSAGE] #{channel.name}")
                print(f"Author: {message.author}")

                await process_message(message)

        last_checked = newest_seen
        await run_publish_checks()
        await asyncio.sleep(SCAN_INTERVAL)


async def main():
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing.")

    print("[NEWSROOM] Starting up...")
    print("[NEWSROOM] Waiting 10 seconds before Discord login...")
    await asyncio.sleep(10)
    await client.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
