"""
event_classifier.py

Aggressive first-pass event classifier for The Newsroom bot.

Goal:
- catch as many potentially relevant baseball news signals as possible
- classify them into event types
- assign confidence / priority
- leave room to tighten rules later

This file is intentionally aggressive and can be tuned down once real traffic is observed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import List, Optional


# =========================
# Event Types
# =========================
EVENT_INJURY = "INJURY"
EVENT_ROLE_CHANGE = "ROLE_CHANGE"
EVENT_CALL_UP = "CALL_UP"
EVENT_DEMOTION = "DEMOTION"
EVENT_LINEUP_CHANGE = "LINEUP_CHANGE"
EVENT_PERFORMANCE = "PERFORMANCE"
EVENT_TRANSACTION = "TRANSACTION"
EVENT_RUMOR = "RUMOR"
EVENT_UNKNOWN = "UNKNOWN"


# =========================
# Keyword Groups
# =========================
INJURY_PATTERNS = [
    r"left the game",
    r"exited the game",
    r"removed from the game",
    r"removed from game",
    r"visited by trainers?",
    r"trainer visit",
    r"being evaluated",
    r"undergoing imaging",
    r"undergo imaging",
    r"undergo an mri",
    r"undergoes? an mri",
    r"scheduled for imaging",
    r"headed for imaging",
    r"will have imaging",
    r"day-to-day",
    r"day to day",
    r"discomfort",
    r"tightness",
    r"soreness",
    r"strain",
    r"sprain",
    r"inflammation",
    r"bruise",
    r"contusion",
    r"hamstring",
    r"elbow",
    r"forearm",
    r"shoulder",
    r"wrist",
    r"oblique",
    r"back spasms?",
    r"knee",
    r"ankle",
    r"neck stiffness",
    r"illness",
    r"flu-like symptoms?",
    r"scratched.*(injur|ill|sore|tight|discomfort|bruise|contusion)",
]

ROLE_CHANGE_PATTERNS = [
    r"will handle saves?",
    r"gets? the next save chance",
    r"ninth inning role",
    r"closer by committee",
    r"closer committee",
    r"named the closer",
    r"expected to close",
    r"preferred option for saves?",
    r"will start",
    r"joining the rotation",
    r"moving into the rotation",
    r"taking over as the starter",
    r"expected to be the everyday",
    r"everyday role",
    r"full-time role",
    r"regular playing time",
    r"batting leadoff",
    r"batting second",
    r"batting third",
    r"batting cleanup",
]

CALL_UP_PATTERNS = [
    r"called up",
    r"recalled from",
    r"promoted from",
    r"promotion",
    r"making his debut",
    r"making her debut",
    r"set to debut",
    r"top prospect",
    r"joining the major league roster",
    r"selected the contract of",
    r"added to the active roster",
]

DEMOTION_PATTERNS = [
    r"optioned to",
    r"sent down",
    r"demoted to",
    r"returned to triple-a",
    r"returned to aaa",
    r"outrighted to",
]

LINEUP_CHANGE_PATTERNS = [
    r"scratched from the lineup",
    r"scratched from lineup",
    r"not in the lineup",
    r"out of the lineup",
    r"batting leadoff",
    r"batting second",
    r"batting third",
    r"batting cleanup",
    r"starting at",
    r"getting the day off",
    r"rest day",
    r"late scratch",
]

PERFORMANCE_PATTERNS = [
    r"2 hr",
    r"3 hr",
    r"two home runs",
    r"three home runs",
    r"10 k",
    r"11 k",
    r"12 k",
    r"struck out \d+",
    r"\d{3}\.\d mph",
    r"exit velocity",
    r"hardest-hit",
    r"hardest hit",
    r"barrel",
    r"whiff rate",
    r"swinging strikes?",
    r"scoreless innings?",
]

TRANSACTION_PATTERNS = [
    r"placed on the injured list",
    r"placed on the il",
    r"placed on il",
    r"activated from the injured list",
    r"activated from the il",
    r"reinstated from the il",
    r"designated for assignment",
    r"dfa",
    r"traded to",
    r"acquired",
    r"dealt to",
    r"signed with",
    r"released",
    r"waived",
    r"claimed off waivers",
    r"selected the contract of",
]

RUMOR_PATTERNS = [
    r"could be",
    r"might be",
    r"possible",
    r"appears likely",
    r"expected to",
    r"sounds like",
    r"worth monitoring",
    r"something to watch",
    r"keep an eye on",
    r"not ideal",
    r"may need",
    r"likely to",
]


# =========================
# Data Model
# =========================
@dataclass
class ClassificationResult:
    raw_text: str
    normalized_text: str
    event_type: str
    confidence: float
    priority: str
    matched_keywords: List[str]
    needs_followup: bool
    is_flash_candidate: bool
    notes: List[str]

    def to_dict(self) -> dict:
        return asdict(self)


# =========================
# Helpers
# =========================
def normalize_text(text: str) -> str:
    text = text or ""
    text = text.replace("\n", " ")
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def find_matches(text: str, patterns: List[str]) -> List[str]:
    matches: List[str] = []
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            matches.append(pattern)
    return matches


def calc_confidence(base: float, match_count: int, bonus: float = 0.0) -> float:
    confidence = base + (0.07 * min(match_count, 4)) + bonus
    return round(min(confidence, 0.99), 2)


def choose_priority(
    event_type: str,
    text: str,
    match_count: int,
) -> str:
    if event_type == EVENT_INJURY:
        if re.search(r"forearm|elbow|shoulder|mri|imaging|placed on il|injured list", text):
            return "HIGH"
        if re.search(r"day-to-day|day to day|tightness|soreness|bruise|contusion|illness", text):
            return "MEDIUM"
        return "HIGH" if match_count >= 2 else "MEDIUM"

    if event_type == EVENT_ROLE_CHANGE:
        if re.search(r"closer|save chance|ninth inning|everyday role|will start|rotation", text):
            return "HIGH"
        return "MEDIUM"

    if event_type in {EVENT_CALL_UP, EVENT_DEMOTION, EVENT_TRANSACTION}:
        return "HIGH"

    if event_type == EVENT_LINEUP_CHANGE:
        if re.search(r"scratched|late scratch", text):
            return "HIGH"
        return "MEDIUM"

    if event_type == EVENT_PERFORMANCE:
        if re.search(r"113\.|114\.|115\.|116\.|10 k|11 k|12 k|two home runs|three home runs", text):
            return "HIGH"
        return "MEDIUM"

    if event_type == EVENT_RUMOR:
        return "LOW"

    return "LOW"


def is_flash_candidate(event_type: str, text: str) -> bool:
    if event_type == EVENT_INJURY and re.search(
        r"left the game|exited the game|removed from the game|trainer visit|visited by trainers?|scratched from the lineup|scratched from lineup",
        text,
    ):
        return True

    if event_type == EVENT_ROLE_CHANGE and re.search(
        r"will handle saves?|gets? the next save chance|entered the ninth|ninth inning role|named the closer",
        text,
    ):
        return True

    if event_type == EVENT_LINEUP_CHANGE and re.search(
        r"late scratch|scratched from the lineup|scratched from lineup",
        text,
    ):
        return True

    return False


# =========================
# Main Classifier
# =========================
def classify_event(raw_text: str) -> ClassificationResult:
    text = normalize_text(raw_text)
    notes: List[str] = []

    injury_matches = find_matches(text, INJURY_PATTERNS)
    role_matches = find_matches(text, ROLE_CHANGE_PATTERNS)
    callup_matches = find_matches(text, CALL_UP_PATTERNS)
    demotion_matches = find_matches(text, DEMOTION_PATTERNS)
    lineup_matches = find_matches(text, LINEUP_CHANGE_PATTERNS)
    performance_matches = find_matches(text, PERFORMANCE_PATTERNS)
    transaction_matches = find_matches(text, TRANSACTION_PATTERNS)
    rumor_matches = find_matches(text, RUMOR_PATTERNS)

    # Priority order matters.
    # Aggressive mode: prefer stronger baseball-event buckets first.
    if injury_matches:
        event_type = EVENT_INJURY
        matched = injury_matches
        confidence = calc_confidence(0.72, len(matched))
        notes.append("Matched injury patterns.")

    elif transaction_matches:
        event_type = EVENT_TRANSACTION
        matched = transaction_matches
        confidence = calc_confidence(0.78, len(matched))
        notes.append("Matched transaction patterns.")

    elif callup_matches:
        event_type = EVENT_CALL_UP
        matched = callup_matches
        confidence = calc_confidence(0.78, len(matched))
        notes.append("Matched call-up patterns.")

    elif demotion_matches:
        event_type = EVENT_DEMOTION
        matched = demotion_matches
        confidence = calc_confidence(0.78, len(matched))
        notes.append("Matched demotion patterns.")

    elif role_matches:
        event_type = EVENT_ROLE_CHANGE
        matched = role_matches
        confidence = calc_confidence(0.7, len(matched))
        notes.append("Matched role-change patterns.")

    elif lineup_matches:
        event_type = EVENT_LINEUP_CHANGE
        matched = lineup_matches
        confidence = calc_confidence(0.68, len(matched))
        notes.append("Matched lineup-change patterns.")

    elif performance_matches:
        event_type = EVENT_PERFORMANCE
        matched = performance_matches
        confidence = calc_confidence(0.64, len(matched))
        notes.append("Matched performance patterns.")

    elif rumor_matches:
        event_type = EVENT_RUMOR
        matched = rumor_matches
        confidence = calc_confidence(0.45, len(matched))
        notes.append("Matched rumor/speculation patterns.")

    else:
        event_type = EVENT_UNKNOWN
        matched = []
        confidence = 0.1
        notes.append("No event patterns matched.")

    priority = choose_priority(event_type, text, len(matched))
    flash = is_flash_candidate(event_type, text)

    needs_followup = event_type in {
        EVENT_INJURY,
        EVENT_ROLE_CHANGE,
        EVENT_CALL_UP,
        EVENT_LINEUP_CHANGE,
        EVENT_TRANSACTION,
        EVENT_RUMOR,
    }

    if event_type == EVENT_PERFORMANCE and priority == "HIGH":
        needs_followup = False

    return ClassificationResult(
        raw_text=raw_text,
        normalized_text=text,
        event_type=event_type,
        confidence=confidence,
        priority=priority,
        matched_keywords=matched,
        needs_followup=needs_followup,
        is_flash_candidate=flash,
        notes=notes,
    )


# =========================
# Quick Test Harness
# =========================
if __name__ == "__main__":
    samples = [
        "Paul Skenes left the game after being visited by trainers.",
        "JT Realmuto was scratched from the lineup with a bruised foot and is day-to-day.",
        "The Orioles promoted Jackson Holliday from Triple-A.",
        "A.J. Puk appears likely to get the next save chance for Miami.",
        "Yordan Alvarez hit two home runs and posted a 114.8 mph exit velocity.",
        "The Braves placed Spencer Strider on the 15-day IL.",
        "Bryce Elder will start Tuesday with Strider sidelined.",
    ]

    for sample in samples:
        result = classify_event(sample)
        print("=" * 80)
        print(sample)
        print(result.to_dict())
