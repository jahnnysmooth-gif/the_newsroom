import discord
import asyncio
import os
import sys
from datetime import datetime, UTC

TOKEN = os.getenv("DISCORD_TOKEN")

CHANNEL_IDS = [
    1482543088223391785,  # beat-writers
    1480361783238852618,  # closer-news
]

SCAN_INTERVAL = 15  # seconds

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

last_checked = datetime.now(UTC)


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

                if message.content:
                    print(f"Text: {message.content}")

                for embed in message.embeds:
                    print(f"[EMBED TITLE] {embed.title}")
                    print(f"[EMBED DESC] {embed.description}")
                    print(f"[EMBED URL] {embed.url}")

        last_checked = newest_seen
        await asyncio.sleep(SCAN_INTERVAL)


async def main():
    if not TOKEN:
        print("[FATAL] DISCORD_TOKEN is missing.")
        sys.exit(1)

    print("[NEWSROOM] Starting up...")
    print("[NEWSROOM] Waiting 10 seconds before Discord login to avoid rate-limit issues...")
    await asyncio.sleep(10)

    try:
        await client.start(TOKEN)
    except discord.HTTPException as e:
        print(f"[FATAL] Discord HTTPException during login/start: {e}")
        raise
    except discord.LoginFailure:
        print("[FATAL] Invalid Discord token.")
        raise
    except Exception as e:
        print(f"[FATAL] Unexpected startup error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
