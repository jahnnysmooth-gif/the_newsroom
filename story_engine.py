"""
story_engine.py

Story engine for The Newsroom bot.

Purpose:
- group incoming items into stories
- merge updates for the same player/event
- manage flash / blurb / follow-up timing
- support default 5-minute hold before posting
- allow fast exceptions for:
  - in-game injuries
  - closer / ninth-inning role changes
  - late lineup scratches

This file is designed to be easy to test and tighten later.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, List, Optional


# =========================
# Event constants
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
# Timing rules
# =========================
DEFAULT_BLURB_WAIT_SECONDS = 300  # 5 minutes

BLURB_WINDOWS = {
    EVENT_INJURY: (180, 480),         # 3 to 8 minutes
    EVENT_LINEUP_CHANGE: (60, 180),   # 1 to 3 minutes
    EVENT_ROLE_CHANGE: (300, 600),    # 5 to 10 minutes
    EVENT_TRANSACTION: (0, 120),      # 0 to 2 minutes
    EVENT_CALL_UP: (180, 420),        # 3 to 7 minutes
    EVENT_DEMOTION: (180, 420),       # 3 to 7 minutes
    EVENT_PERFORMANCE: (0, 120),      # fast if needed
    EVENT_RUMOR: (300, 600),          # wait longer
    EVENT_UNKNOWN: (300, 300),
}

STORY_CLOSE_WINDOWS = {
    EVENT_INJURY: 7200,         # 2 hours
    EVENT_LINEUP_CHANGE: 10800, # 3 hours
    EVENT_ROLE_CHANGE: 14400,   # 4 hours
    EVENT_TRANSACTION: 1800,    # 30 minutes
    EVENT_CALL_UP: 86400,       # 24 hours
    EVENT_DEMOTION: 86400,      # 24 hours
    EVENT_PERFORMANCE: 3600,    # 1 hour
    EVENT_RUMOR: 3600,          # 1 hour
    EVENT_UNKNOWN: 1800,
}


# =========================
# Data models
# =========================
@dataclass
class StoryItem:
    text: str
    source: str
    source_type: str
    timestamp: datetime
    url: Optional[str] = None
    confidence: float = 0.0
    priority: str = "LOW"
    matched_keywords: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "text": self.text,
            "source": self.source,
            "source_type": self.source_type,
            "timestamp": self.timestamp.isoformat(),
            "url": self.url,
            "confidence": self.confidence,
            "priority": self.priority,
            "matched_keywords": list(self.matched_keywords),
        }


@dataclass
class Story:
    story_id: str
    player_name: str
    event_type: str
    team: Optional[str]
    created_at: datetime
    updated_at: datetime
    flash_posted: bool = False
    blurb_posted: bool = False
    followup_posted: bool = False
    is_flash_candidate: bool = False
    needs_followup: bool = True
    confidence: float = 0.0
    priority: str = "LOW"
    status: str = "OPEN"
    facts: List[str] = field(default_factory=list)
    items: List[StoryItem] = field(default_factory=list)
    source_names: List[str] = field(default_factory=list)
    matched_keywords: List[str] = field(default_factory=list)
    first_flash_at: Optional[datetime] = None
    blurb_eligible_at: Optional[datetime] = None
    close_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "story_id": self.story_id,
            "player_name": self.player_name,
            "event_type": self.event_type,
            "team": self.team,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "flash_posted": self.flash_posted,
            "blurb_posted": self.blurb_posted,
            "followup_posted": self.followup_posted,
            "is_flash_candidate": self.is_flash_candidate,
            "needs_followup": self.needs_followup,
            "confidence": self.confidence,
            "priority": self.priority,
            "status": self.status,
            "facts": list(self.facts),
            "items": [item.to_dict() for item in self.items],
            "source_names": list(self.source_names),
            "matched_keywords": list(self.matched_keywords),
            "first_flash_at": self.first_flash_at.isoformat() if self.first_flash_at else None,
            "blurb_eligible_at": self.blurb_eligible_at.isoformat() if self.blurb_eligible_at else None,
            "close_at": self.close_at.isoformat() if self.close_at else None,
        }


# =========================
# Helpers
# =========================
def utcnow() -> datetime:
    return datetime.now(UTC)


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


def story_key(player_name: str, event_type: str) -> str:
    safe_player = player_name.lower().replace(" ", "_").replace(".", "").replace("-", "_")
    safe_event = event_type.lower()
    return f"{safe_player}__{safe_event}"


def choose_higher_priority(p1: str, p2: str) -> str:
    order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    return p1 if order.get(p1, 0) >= order.get(p2, 0) else p2


def update_confidence(old_conf: float, new_conf: float) -> float:
    return round(max(old_conf, new_conf), 2)


def build_blurb_eligible_at(
    event_type: str,
    created_at: datetime,
    is_flash_candidate: bool,
) -> datetime:
    min_wait, _ = BLURB_WINDOWS.get(event_type, (DEFAULT_BLURB_WAIT_SECONDS, DEFAULT_BLURB_WAIT_SECONDS))

    # Your rule:
    # - default wait at least 5 minutes
    # - exceptions can go faster
    if event_type in {EVENT_INJURY, EVENT_ROLE_CHANGE, EVENT_LINEUP_CHANGE} and is_flash_candidate:
        return created_at + timedelta(seconds=min_wait)

    # everyone else waits at least 5 minutes
    return created_at + timedelta(seconds=DEFAULT_BLURB_WAIT_SECONDS)


def build_close_at(event_type: str, created_at: datetime) -> datetime:
    seconds = STORY_CLOSE_WINDOWS.get(event_type, 1800)
    return created_at + timedelta(seconds=seconds)


def should_merge_story(
    story: Story,
    player_name: str,
    event_type: str,
    now: datetime,
) -> bool:
    if story.player_name != player_name:
        return False
    if story.event_type != event_type:
        return False
    if story.status == "CLOSED":
        return False
    if story.close_at and now > story.close_at:
        return False
    return True


def extract_fact_candidates(text: str, matched_keywords: List[str]) -> List[str]:
    facts = []
    lowered = (text or "").strip()

    # Keep the raw text itself as a candidate fact for now.
    # Later we can turn this into clean structured fact extraction.
    if lowered:
        facts.append(lowered)

    for kw in matched_keywords:
        if kw:
            facts.append(kw)

    return dedupe_keep_order(facts)


# =========================
# Story engine
# =========================
class StoryEngine:
    def __init__(self) -> None:
        self.active_stories: Dict[str, Story] = {}

    def ingest(
        self,
        *,
        player_name: str,
        event_type: str,
        raw_text: str,
        source: str,
        source_type: str,
        timestamp: Optional[datetime] = None,
        team: Optional[str] = None,
        confidence: float = 0.0,
        priority: str = "LOW",
        matched_keywords: Optional[List[str]] = None,
        is_flash_candidate: bool = False,
        needs_followup: bool = True,
        url: Optional[str] = None,
    ) -> Story:
        now = timestamp or utcnow()
        matched_keywords = matched_keywords or []

        existing = self._find_existing_story(player_name, event_type, now)

        item = StoryItem(
            text=raw_text,
            source=source,
            source_type=source_type,
            timestamp=now,
            url=url,
            confidence=confidence,
            priority=priority,
            matched_keywords=matched_keywords,
        )

        if existing:
            self._merge_item(existing, item, matched_keywords, confidence, priority, is_flash_candidate, needs_followup)
            return existing

        new_story = self._create_story(
            player_name=player_name,
            event_type=event_type,
            team=team,
            item=item,
            confidence=confidence,
            priority=priority,
            matched_keywords=matched_keywords,
            is_flash_candidate=is_flash_candidate,
            needs_followup=needs_followup,
        )
        self.active_stories[new_story.story_id] = new_story
        return new_story

    def _find_existing_story(self, player_name: str, event_type: str, now: datetime) -> Optional[Story]:
        for story in self.active_stories.values():
            if should_merge_story(story, player_name, event_type, now):
                return story
        return None

    def _create_story(
        self,
        *,
        player_name: str,
        event_type: str,
        team: Optional[str],
        item: StoryItem,
        confidence: float,
        priority: str,
        matched_keywords: List[str],
        is_flash_candidate: bool,
        needs_followup: bool,
    ) -> Story:
        sid = story_key(player_name, event_type)
        created_at = item.timestamp

        story = Story(
            story_id=sid,
            player_name=player_name,
            event_type=event_type,
            team=team,
            created_at=created_at,
            updated_at=created_at,
            is_flash_candidate=is_flash_candidate,
            needs_followup=needs_followup,
            confidence=round(confidence, 2),
            priority=priority,
            status="OPEN",
            facts=extract_fact_candidates(item.text, matched_keywords),
            items=[item],
            source_names=[item.source],
            matched_keywords=list(matched_keywords),
            blurb_eligible_at=build_blurb_eligible_at(event_type, created_at, is_flash_candidate),
            close_at=build_close_at(event_type, created_at),
        )

        return story

    def _merge_item(
        self,
        story: Story,
        item: StoryItem,
        matched_keywords: List[str],
        confidence: float,
        priority: str,
        is_flash_candidate: bool,
        needs_followup: bool,
    ) -> None:
        story.items.append(item)
        story.updated_at = item.timestamp
        story.source_names = dedupe_keep_order(story.source_names + [item.source])
        story.matched_keywords = dedupe_keep_order(story.matched_keywords + matched_keywords)
        story.facts = dedupe_keep_order(story.facts + extract_fact_candidates(item.text, matched_keywords))
        story.confidence = update_confidence(story.confidence, confidence)
        story.priority = choose_higher_priority(story.priority, priority)
        story.is_flash_candidate = story.is_flash_candidate or is_flash_candidate
        story.needs_followup = story.needs_followup or needs_followup
        story.close_at = max(story.close_at or item.timestamp, build_close_at(story.event_type, item.timestamp))

    # =========================
    # Publishing decisions
    # =========================
    def get_flash_ready_stories(self, now: Optional[datetime] = None) -> List[Story]:
        now = now or utcnow()
        ready = []

        for story in self.active_stories.values():
            if story.status == "CLOSED":
                continue
            if story.flash_posted:
                continue
            if not story.is_flash_candidate:
                continue

            # flash immediately once we see the story
            ready.append(story)

        return ready

    def mark_flash_posted(self, story_id: str, now: Optional[datetime] = None) -> None:
        story = self.active_stories.get(story_id)
        if not story:
            return
        story.flash_posted = True
        story.first_flash_at = now or utcnow()

    def get_blurb_ready_stories(self, now: Optional[datetime] = None) -> List[Story]:
        now = now or utcnow()
        ready = []

        for story in self.active_stories.values():
            if story.status == "CLOSED":
                continue
            if story.blurb_posted:
                continue
            if not story.blurb_eligible_at:
                continue
            if now < story.blurb_eligible_at:
                continue

            # Must have some content to summarize
            if not story.facts and not story.items:
                continue

            ready.append(story)

        # Highest priority first
        ready.sort(key=lambda s: (self._priority_rank(s.priority), s.created_at), reverse=True)
        return ready

    def mark_blurb_posted(self, story_id: str) -> None:
        story = self.active_stories.get(story_id)
        if not story:
            return
        story.blurb_posted = True

    def get_followup_ready_stories(self, now: Optional[datetime] = None) -> List[Story]:
        now = now or utcnow()
        ready = []

        for story in self.active_stories.values():
            if story.status == "CLOSED":
                continue
            if not story.blurb_posted:
                continue
            if story.followup_posted:
                continue
            if not story.needs_followup:
                continue
            if len(story.items) < 2:
                continue

            # crude first pass:
            # if we already posted a blurb and got at least one more source/item after that,
            # make it follow-up eligible
            if story.updated_at > story.created_at:
                ready.append(story)

        return ready

    def mark_followup_posted(self, story_id: str) -> None:
        story = self.active_stories.get(story_id)
        if not story:
            return
        story.followup_posted = True

    def close_expired_stories(self, now: Optional[datetime] = None) -> List[str]:
        now = now or utcnow()
        closed = []

        for story in self.active_stories.values():
            if story.status == "CLOSED":
                continue
            if story.close_at and now >= story.close_at:
                story.status = "CLOSED"
                closed.append(story.story_id)

        return closed

    def get_story(self, story_id: str) -> Optional[Story]:
        return self.active_stories.get(story_id)

    def list_open_stories(self) -> List[Story]:
        return [story for story in self.active_stories.values() if story.status != "CLOSED"]

    def export_state(self) -> Dict[str, Any]:
        return {sid: story.to_dict() for sid, story in self.active_stories.items()}

    @staticmethod
    def _priority_rank(priority: str) -> int:
        return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(priority, 0)


# =========================
# Quick test harness
# =========================
if __name__ == "__main__":
    engine = StoryEngine()
    now = utcnow()

    # In-game injury -> flash immediately, blurb soon
    s1 = engine.ingest(
        player_name="Paul Skenes",
        event_type=EVENT_INJURY,
        raw_text="Paul Skenes left the game after being visited by trainers.",
        source="Alex Stumpf",
        source_type="beat_writer",
        timestamp=now,
        confidence=0.92,
        priority="HIGH",
        matched_keywords=["left the game", "visited by trainers"],
        is_flash_candidate=True,
        needs_followup=True,
    )

    # More context same story
    engine.ingest(
        player_name="Paul Skenes",
        event_type=EVENT_INJURY,
        raw_text="Skenes is dealing with forearm discomfort and will be evaluated further.",
        source="Jeff Passan",
        source_type="national_reporter",
        timestamp=now + timedelta(minutes=2),
        confidence=0.97,
        priority="HIGH",
        matched_keywords=["forearm", "discomfort", "being evaluated"],
        is_flash_candidate=False,
        needs_followup=True,
    )

    # Default story -> wait 5 min
    s2 = engine.ingest(
        player_name="Jackson Holliday",
        event_type=EVENT_CALL_UP,
        raw_text="The Orioles promoted Jackson Holliday from Triple-A.",
        source="Orioles PR",
        source_type="transactions",
        timestamp=now,
        confidence=0.96,
        priority="HIGH",
        matched_keywords=["promoted from", "called up"],
        is_flash_candidate=False,
        needs_followup=True,
    )

    print("\n=== FLASH READY NOW ===")
    for story in engine.get_flash_ready_stories(now):
        print(story.story_id, story.player_name, story.event_type)

    print("\n=== BLURB READY AT +4 MIN ===")
    for story in engine.get_blurb_ready_stories(now + timedelta(minutes=4)):
        print(story.story_id, story.player_name, story.event_type)

    print("\n=== BLURB READY AT +6 MIN ===")
    for story in engine.get_blurb_ready_stories(now + timedelta(minutes=6)):
        print(story.story_id, story.player_name, story.event_type)

    print("\n=== STATE EXPORT ===")
    print(engine.export_state())
