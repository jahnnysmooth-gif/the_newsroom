# ESPN Fantasy Player News Bot Starter

This is a starter Discord bot built around the ESPN Fantasy Baseball player news page.

## What this version already does
- Opens the ESPN player news page with Playwright
- Extracts likely news cards using fallback selectors
- Normalizes each item into a simple news model
- Classifies items into categories like injury, lineup, role, transaction, or general
- Deduplicates posts with stored IDs and text hashes
- Posts clean Discord embeds
- Includes `!espncheck` and `!espnpreview` test commands

## Why this version uses Playwright
The ESPN player news page is not reliably populated in raw HTML, so this starter uses a rendered-page approach. The public page itself is sparse in non-rendered HTML, which is why a plain `requests` scrape is likely to be fragile. See the page capture here: `https://fantasy.espn.com/baseball/playernews`.

## Important note
This is a strong starter foundation, but you will still want to inspect ESPN's live DOM in your own browser once and tighten the selectors in `espn_news_bot.py`.

The most likely next improvements are:
1. tighten the card, title, and body selectors after one live DOM inspection
2. add MLB API-backed team and position resolution
3. split the rewrite layer into better `update` and `fantasy impact` logic
4. add thumbnail support later
5. add routing to different Discord channels by category

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env
```

Then set your environment variables and run:

```bash
python espn_news_bot.py
```

## Notes on testing
Use these Discord commands once the bot is online:
- `!espnpreview` to preview the first extracted item
- `!espncheck` to run a manual poll cycle

## State files
The bot stores files in `state/espn_news/`:
- `posted_ids.json`
- `recent_hashes.json`
- `source_health.json`
- `player_cache.json`

## Recommended next pass
The next pass should lock down the ESPN selectors using the current DOM and then add a player resolver using the MLB Stats API.
