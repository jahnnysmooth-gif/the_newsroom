import discord
import asyncio
import os
from datetime import datetime

TOKEN = os.getenv("DISCORD_TOKEN")

# 🔴 REPLACE THESE WITH YOUR CHANNEL IDs
CHANNEL_IDS = [
    1482543088223391785,  # beat-writers
    1480361783238852618,  # closer-news
]

SCAN_INTERVAL = 15  # seconds

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

last_checked = datetime.utcnow()


@client.event
async def on_ready():
    print(f"[NEWSROOM] Logged in as {client.user}")
    client.loop.create_task(batch_loop())


async def batch_loop():
    global last_checked

    await client.wait_until_ready()

    while not client.is_closed():
        print(f"\n[SCAN] {datetime.utcnow()}")

        for channel_id in CHANNEL_IDS:
            channel = client.get_channel(channel_id)

            if not channel:
                print(f"[ERROR] Channel not found: {channel_id}")
                continue

            async for message in channel.history(limit=50):
                if message.created_at <= last_checked:
                    break

                print(f"\n[NEW MESSAGE] #{channel.name}")
                print(f"Author: {message.author}")

                if message.content:
                    print(f"Text: {message.content}")

                # Handle embeds (tweets)
                for embed in message.embeds:
                    print(f"[EMBED TITLE] {embed.title}")
                    print(f"[EMBED DESC] {embed.description}")

        last_checked = datetime.utcnow()
        await asyncio.sleep(SCAN_INTERVAL)


client.run(TOKEN)
