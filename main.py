import discord
import asyncio
import os
from datetime import datetime, UTC

TOKEN = os.getenv("DISCORD_TOKEN")

# 🔴 YOUR CHANNELS (already correct)
CHANNEL_IDS = [
    1482543088223391785,  # beat-writers
    1480361783238852618,  # closer-news
]

SCAN_INTERVAL = 15  # seconds

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

# ✅ FIX: use timezone-aware datetime
last_checked = datetime.now(UTC)


@client.event
async def on_ready():
    print(f"[NEWSROOM] Logged in as {client.user}")
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
                # ✅ FIX: safe datetime comparison
                if message.created_at <= last_checked:
                    break

                # track newest message we’ve seen
                if message.created_at > newest_seen:
                    newest_seen = message.created_at

                print(f"\n[NEW MESSAGE] #{channel.name}")
                print(f"Author: {message.author}")

                if message.content:
                    print(f"Text: {message.content}")

                # Handle embeds (tweets)
                for embed in message.embeds:
                    print(f"[EMBED TITLE] {embed.title}")
                    print(f"[EMBED DESC] {embed.description}")
                    print(f"[EMBED URL] {embed.url}")

        # ✅ update AFTER loop
        last_checked = newest_seen

        await asyncio.sleep(SCAN_INTERVAL)


client.run(TOKEN)
