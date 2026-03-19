"""
player_matcher.py

Aggressive player matcher for The Newsroom bot.

Goal:
- detect MLB player names from tweet text
- support full names + last names
- handle multiple players in one message
- return structured matches
"""

import re
from typing import List, Dict

# 🔴 For now we hardcode a small sample list
# Later we will expand this to top 350 players (your priority)

PLAYERS = [
    "Paul Skenes",
    "JT Realmuto",
    "Bryce Elder",
    "Jackson Holliday",
    "Yordan Alvarez",
    "Spencer Strider",
    "A.J. Puk",
]

# Build lookup maps
FULL_NAME_MAP = {p.lower(): p for p in PLAYERS}
LAST_NAME_MAP = {p.split()[-1].lower(): p for p in PLAYERS}


def normalize_text(text: str) -> str:
    text = text or ""
    text = text.lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[^a-z\s\.]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def find_full_name_matches(text: str) -> List[str]:
    matches = []
    for full_lower, full_name in FULL_NAME_MAP.items():
        if full_lower in text:
            matches.append(full_name)
    return matches


def find_last_name_matches(text: str) -> List[str]:
    matches = []
    words = text.split()

    for word in words:
        if word in LAST_NAME_MAP:
            matches.append(LAST_NAME_MAP[word])

    return matches


def dedupe(players: List[str]) -> List[str]:
    seen = set()
    result = []

    for p in players:
        if p not in seen:
            seen.add(p)
            result.append(p)

    return result


def match_players(raw_text: str) -> List[Dict]:
    text = normalize_text(raw_text)

    full_matches = find_full_name_matches(text)
    last_matches = find_last_name_matches(text)

    players = dedupe(full_matches + last_matches)

    results = []

    for p in players:
        results.append({
            "name": p,
            "match_type": "full" if p.lower() in text else "last"
        })

    return results


# =========================
# Test
# =========================
if __name__ == "__main__":
    samples = [
        "Paul Skenes left the game with elbow discomfort.",
        "Realmuto was scratched from the lineup.",
        "Strider placed on IL, Elder will start.",
        "Jackson Holliday called up by Orioles.",
        "Yordan Alvarez hits two HRs.",
    ]

    for s in samples:
        print("=" * 60)
        print(s)
        print(match_players(s))
