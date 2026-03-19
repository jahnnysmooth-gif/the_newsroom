"""
blurb_generator.py

Generates short, fantasy-focused blurbs from Story objects.

Design goals:
- Rotoworld-style rhythm
- short, clean, readable
- 2-3 sentences max
- no drop advice
- add advice only when role clarity is obvious
"""

from __future__ import annotations

import re
from typing import List, Optional


def dedupe_keep_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def safe_join_sources(source_names: List[str], max_sources: int = 3) -> str:
    cleaned = dedupe_keep_order(source_names)
    return " / ".join(cleaned[:max_sources])


def clean_fact_text(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def infer_top_fact(facts: List[str], fallback: str = "") -> str:
    cleaned = [clean_fact_text(f) for f in facts if clean_fact_text(f)]
    if cleaned:
        return cleaned[0]
    return fallback


def has_any(text: str, keywords: List[str]) -> bool:
    lowered = (text or "").lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def title_line(player_name: str, team: Optional[str]) -> str:
    if team:
        return f"**{player_name} ({team})**"
    return f"**{player_name}**"


def should_recommend_add(story) -> bool:
    text_blob = " ".join(story.facts + [item.text for item in story.items]).lower()

    if story.event_type == "CALL_UP":
        if any(k in text_blob for k in [
            "everyday",
            "regular playing time",
            "full-time role",
            "take over as the everyday",
            "expected to start",
            "joining the rotation",
            "will start",
            "batting leadoff",
            "batting second",
            "batting third",
            "batting cleanup",
        ]):
            return True

    if story.event_type == "ROLE_CHANGE":
        if any(k in text_blob for k in [
            "will handle saves",
            "next save chance",
            "named the closer",
            "ninth inning role",
            "expected to close",
            "preferred option for saves",
            "everyday role",
            "full-time role",
        ]):
            return True

    if story.event_type == "TRANSACTION":
        if any(k in text_blob for k in [
            "called up",
            "promoted",
            "selected the contract",
            "joining the rotation",
            "expected to start",
            "everyday role",
        ]):
            return True

    return False


def build_injury_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} is dealing with an injury issue.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
    ]

    text_blob = " ".join(story.facts + [item.text for item in story.items]).lower()

    if has_any(text_blob, ["mri", "imaging", "forearm", "elbow", "shoulder"]):
        lines.append("The situation appears serious enough to warrant close fantasy monitoring.")
    elif has_any(text_blob, ["day-to-day", "day to day", "bruise", "contusion", "illness"]):
        lines.append("His status appears less severe for now, but availability still needs to be monitored.")
    else:
        lines.append("More information should follow once the team provides an update.")

    return "\n".join(lines)


def build_role_change_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} appears to be moving into a larger role.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
    ]

    if should_recommend_add(story):
        lines.append(f"{story.player_name} should be added in fantasy leagues where that role has clear value.")
    else:
        lines.append("This is a role change fantasy managers will want to monitor closely.")

    return "\n".join(lines)


def build_callup_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} has been promoted.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
    ]

    if should_recommend_add(story):
        lines.append(f"{story.player_name} should be added in fantasy leagues if he is still available.")
    else:
        lines.append("His expected role will determine how quickly he becomes fantasy-relevant.")

    return "\n".join(lines)


def build_demotion_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} has been sent down.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
        "This move reduces his short-term fantasy value until he returns to a meaningful role.",
    ]
    return "\n".join(lines)


def build_lineup_change_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} had a lineup-related update.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
    ]

    text_blob = " ".join(story.facts + [item.text for item in story.items]).lower()

    if has_any(text_blob, ["scratched", "late scratch", "not in the lineup", "out of the lineup"]):
        lines.append("Fantasy managers should keep an eye on the next lineup card for clarification.")
    else:
        lines.append("A lineup shift like this can change short-term fantasy value, especially in daily formats.")

    return "\n".join(lines)


def build_performance_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} turned in a notable performance.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
        "This is the type of performance signal fantasy managers should keep on the radar.",
    ]
    return "\n".join(lines)


def build_transaction_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"{story.player_name} was involved in a transaction.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
    ]

    if should_recommend_add(story):
        lines.append(f"{story.player_name} should be added in leagues where the new role creates immediate value.")
    else:
        lines.append("The fantasy impact will depend on how this move affects his role and playing time.")

    return "\n".join(lines)


def build_rumor_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"There is a developing report involving {story.player_name}.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
        "This is still developing, so fantasy managers should wait for stronger confirmation.",
    ]
    return "\n".join(lines)


def build_unknown_blurb(story) -> str:
    fact = infer_top_fact(story.facts, fallback=f"There is a new update involving {story.player_name}.")
    lines = [
        title_line(story.player_name, story.team),
        "",
        clean_fact_text(fact),
        "More context may be needed before the fantasy impact becomes clear.",
    ]
    return "\n".join(lines)


def generate_blurb(story) -> str:
    event_type = getattr(story, "event_type", "UNKNOWN")

    if event_type == "INJURY":
        body = build_injury_blurb(story)
    elif event_type == "ROLE_CHANGE":
        body = build_role_change_blurb(story)
    elif event_type == "CALL_UP":
        body = build_callup_blurb(story)
    elif event_type == "DEMOTION":
        body = build_demotion_blurb(story)
    elif event_type == "LINEUP_CHANGE":
        body = build_lineup_change_blurb(story)
    elif event_type == "PERFORMANCE":
        body = build_performance_blurb(story)
    elif event_type == "TRANSACTION":
        body = build_transaction_blurb(story)
    elif event_type == "RUMOR":
        body = build_rumor_blurb(story)
    else:
        body = build_unknown_blurb(story)

    sources = safe_join_sources(getattr(story, "source_names", []))
    if sources:
        body += f"\n\n**Source:** {sources}"

    return body


if __name__ == "__main__":
    from types import SimpleNamespace

    sample_story = SimpleNamespace(
        player_name="Jackson Holliday",
        team="BAL",
        event_type="CALL_UP",
        facts=[
            "The Orioles promoted Jackson Holliday from Triple-A and he is expected to take over as the everyday second baseman."
        ],
        items=[],
        source_names=["Orioles PR", "Ken Rosenthal"],
    )

    print(generate_blurb(sample_story))
