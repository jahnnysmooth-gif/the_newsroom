
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import discord
from discord.ext import commands, tasks

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    from playwright.async_api import async_playwright
except Exception:
    async_playwright = None


BASE_DIR = Path(__file__).resolve().parent
if load_dotenv:
    load_dotenv(BASE_DIR / ".env")

try:
    import news_config  # type: ignore
except Exception:
    news_config = None


def cfg(name: str, default: Any = None) -> Any:
    if news_config and hasattr(news_config, name):
        value = getattr(news_config, name)
        if value is not None:
            return value
    return os.getenv(name, default)


NEWS_BOT_TOKEN = str(cfg("NEWS_BOT_TOKEN", "") or "").strip()
NEWS_CHANNEL_ID = int(str(cfg("NEWS_CHANNEL_ID", "0") or "0"))
POLL_MINUTES = int(str(cfg("POLL_MINUTES", "5") or "5"))
MAX_POSTS_PER_RUN = int(str(cfg("MAX_POSTS_PER_RUN", "50") or "50"))
ESPN_URL = str(cfg("ESPN_URL", "https://fantasy.espn.com/baseball/playernews") or "").strip()
HEADLESS = str(cfg("HEADLESS", "true") or "true").lower() not in {"0", "false", "no"}
RESET_STATE_ON_START = str(cfg("RESET_STATE_ON_START", "false") or "false").lower() in {"1", "true", "yes"}

STATE_DIR = BASE_DIR / "state" / "espn_news"
POSTED_IDS_FILE = STATE_DIR / "posted_ids.json"
RECENT_HASHES_FILE = STATE_DIR / "recent_hashes.json"
PLAYER_LAST_POSTS_FILE = STATE_DIR / "player_last_posts.json"
SCRAPE_DEBUG_FILE = STATE_DIR / "last_scrape_candidates.txt"

PLAYER_ID_PATH = BASE_DIR / "shared" / "player_ids" / "espn_player_ids.json"
LOCAL_PLAYER_ID_PATH = BASE_DIR / "espn_player_ids.json"
MLB_FALLBACK_LOGO = "https://a.espncdn.com/i/teamlogos/leagues/500/mlb.png"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

TIMESTAMP_RE = re.compile(
    r"(?:SUN|MON|TUE|WED|THU|FRI|SAT),\s+[A-Z]{3}\s+\d{1,2},\s+\d{1,2}:\d{2}\s+[AP]\.?M\.?",
    re.I,
)

POS_SUFFIX_RE = re.compile(r"(?:C|1B|2B|3B|SS|OF|LF|CF|RF|DH|SP|RP|P)$", re.I)

VALID_POSITIONS = {"C", "1B", "2B", "3B", "SS", "OF", "LF", "CF", "RF", "DH", "SP", "RP", "P"}
INVALID_PLAYER_NAME_EXACT = {
    "batter", "pitcher", "infielder", "outfielder", "catcher", "designated hitter",
    "relief pitcher", "starting pitcher", "player", "mlb", "news archive", "red", "white"
}
INVALID_PLAYER_NAME_WORDS = {
    "athletics", "yankees", "mets", "dodgers", "mariners", "padres", "giants", "pirates",
    "twins", "brewers", "braves", "cardinals", "cubs", "orioles", "rangers", "rockies",
    "phillies", "marlins", "angels", "astros", "nationals", "tigers", "royals", "rays",
    "guardians", "diamondbacks", "red sox", "white sox", "blue jays"
}

NAV_BAD_PATTERNS = [
    "hsb.accessibility.skipcontent",
    "where to watch",
    "fantasy where to watch",
    "espn nfl nba ncaam",
    "soccer more sports watch fantasy",
    "menu espn",
    "search scores",
    "fantasy baseball support",
    "reset draft",
    "member services",
    "privacy policy",
    "terms of use",
]

PERFORMANCE_PATTERNS = [
    r"\bwent \d+-for-\d+\b",
    r"\bhas allowed\b",
    r"\bhas slashed\b",
    r"\bis batting\b",
    r"\bthrough \d+ appearances\b",
    r"\bover \d+(\.\d+)? innings\b",
    r"\bposted a \d+:\d+ k:bb\b",
    r"\bcollected \d+ hits?\b",
    r"\bera\b",
    r"\bops\b",
]

TEAM_WORD_TO_ABBR = {
    "reds": "CIN", "angels": "LAA", "brewers": "MIL", "astros": "HOU",
    "nationals": "WSH", "rockies": "COL", "yankees": "NYY", "dodgers": "LAD",
    "mets": "NYM", "orioles": "BAL", "guardians": "CLE", "twins": "MIN",
    "mariners": "SEA", "phillies": "PHI", "padres": "SD", "pirates": "PIT",
    "cubs": "CHC", "cardinals": "STL", "diamondbacks": "ARI", "blue jays": "TOR",
    "tigers": "DET", "rangers": "TEX", "giants": "SF", "athletics": "ATH",
    "royals": "KC", "marlins": "MIA", "rays": "TB", "red sox": "BOS",
    "white sox": "CWS", "braves": "ATL"
}

TEAM_META = {
    "ARI": {"slug": "ari", "color": 0xA71930},
    "ATH": {"slug": "oak", "color": 0x003831},
    "ATL": {"slug": "atl", "color": 0xCE1141},
    "BAL": {"slug": "bal", "color": 0xDF4601},
    "BOS": {"slug": "bos", "color": 0xBD3039},
    "CHC": {"slug": "chc", "color": 0x0E3386},
    "CWS": {"slug": "chw", "color": 0x27251F},
    "CIN": {"slug": "cin", "color": 0xC6011F},
    "CLE": {"slug": "cle", "color": 0xE31937},
    "COL": {"slug": "col", "color": 0x33006F},
    "DET": {"slug": "det", "color": 0x0C2340},
    "HOU": {"slug": "hou", "color": 0xEB6E1F},
    "KC": {"slug": "kc", "color": 0x004687},
    "LAA": {"slug": "laa", "color": 0xBA0021},
    "LAD": {"slug": "lad", "color": 0x005A9C},
    "MIA": {"slug": "mia", "color": 0x00A3E0},
    "MIL": {"slug": "mil", "color": 0x12284B},
    "MIN": {"slug": "min", "color": 0x002B5C},
    "NYM": {"slug": "nym", "color": 0x002D72},
    "NYY": {"slug": "nyy", "color": 0x132448},
    "PHI": {"slug": "phi", "color": 0xE81828},
    "PIT": {"slug": "pit", "color": 0xFDB827},
    "SD": {"slug": "sd", "color": 0x2F241D},
    "SF": {"slug": "sf", "color": 0xFD5A1E},
    "SEA": {"slug": "sea", "color": 0x0C2C56},
    "STL": {"slug": "stl", "color": 0xC41E3A},
    "TB": {"slug": "tb", "color": 0x092C5C},
    "TEX": {"slug": "tex", "color": 0x003278},
    "TOR": {"slug": "tor", "color": 0x134A8E},
    "WSH": {"slug": "wsh", "color": 0xAB0003},
}


def log(msg: str) -> None:
    print(f"[ESPN NEWS BOT] {msg}", flush=True)


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_json_file(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return default
        return json.loads(raw)
    except Exception as exc:
        log(f"Failed reading {path}: {exc}")
        return default


def save_json_file(path: Path, data: Any) -> None:
    ensure_state_dir()
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def write_text_file(path: Path, text: str) -> None:
    ensure_state_dir()
    path.write_text(text, encoding="utf-8")


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_text(text: str) -> str:
    text = clean_spaces(text).lower()
    text = text.replace("’", "'").replace("“", '"').replace("”", '"')
    text = re.sub(r"[^a-z0-9\s':,()./-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


KNOWN_NAME_FIXES = {
    "Matt Thai": "Matt Thaiss",
    "Matt Thai Red": "Matt Thaiss",
}

def normalize_person_name(text: str) -> str:
    text = clean_spaces(text).lower()
    text = re.sub(r"[^a-z0-9\s.-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def looks_like_nav_or_shell(text: str) -> bool:
    norm = normalize_text(text)
    return not norm or any(p in norm for p in NAV_BAD_PATTERNS)


def strip_reporter_tail(text: str) -> str:
    text = clean_spaces(text)
    patterns = [
        r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+\s+of\s+[^.]+(?:\.)?$",
        r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+\s+reports?\.$",
        r",\s*according to [^.]+(?:\.)?$",
        r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+\s+told\s+MLB\.com\.?$",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text).strip(" ,")
    return clean_spaces(text)


def _words(text: str) -> List[str]:
    return [w for w in re.split(r"\s+", clean_spaces(text).lower()) if w]


def is_valid_player_name(name: str) -> bool:
    cleaned = clean_spaces(name)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in INVALID_PLAYER_NAME_EXACT:
        return False
    if len(cleaned) < 4 or len(cleaned.split()) > 4:
        return False
    words = _words(cleaned)
    if len(words) < 2:
        return False
    if all(w in INVALID_PLAYER_NAME_WORDS for w in words):
        return False
    if any(w in VALID_POSITIONS for w in cleaned.upper().split()):
        return False
    if re.search(r"(?:News Archive|ESPN|Fantasy)", cleaned, re.I):
        return False
    return bool(re.match(r"^[A-Za-z'.-]+(?:\s+[A-Za-z'.-]+){1,3}$", cleaned))


def looks_like_actionable_injury_news(text: str) -> bool:
    norm = normalize_text(text)
    strong_terms = [
        "injured list", " il ", "rehab assignment", "placed on", "expected to begin the regular season on the injured list",
        "will open the season on the injured list", "shut down", "underwent", "surgery", "mri", "x-rays",
        "fracture", "strain", "sprain", "tear", "day to day", "setback", "resume throwing",
        "resume baseball activities", "throwing program", "bullpen session", "sim game", "will miss",
        "expected back", "timeline", "timetable", "re-evaluated", "exam revealed", "diagnosed with",
        "scratched from", "removed from", "left sunday", "left monday", "left tuesday", "left wednesday",
        "left thursday", "left friday", "left saturday", "tightness", "discomfort", "inflammation",
        "play catch", "playing catch", "took batting practice", "taking batting practice",
        "returned to the lineup", "return to the lineup", "available for opening day"
    ]
    weak_context_only = [
        "relief appearance", "out of the bullpen", "in the bullpen", "plans to deploy",
        "projected to start", "fill the vacancy", "more starts"
    ]
    if any(t in norm for t in strong_terms):
        return True
    if any(t in norm for t in weak_context_only):
        return False
    return False


def is_pitcher_box_score_recap(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    if looks_like_actionable_injury_news(text):
        return False
    spring_terms = ["grapefruit league", "cactus league", "spring training", "exhibition"]
    if not any(t in text for t in spring_terms):
        return False
    patterns = [
        r"\ballowed\s+\d+\s+runs?\b",
        r"\ballowed\s+\d+\s+earned runs?\b",
        r"\bon\s+\d+\s+hits?\b",
        r"\bwhile striking out\s+\d+\b",
        r"\bstruck out\s+\d+\b",
        r"\bover\s+\d+(?:\.\d+)?\s+innings\b",
        r"\bin\s+(?:a|an)\s+cactus league start\b",
        r"\bin\s+(?:a|an)\s+grapefruit league start\b",
        r"\btook the loss\b",
        r"\bpicked up the win\b",
        r"\bscoreless inning(?:s)?\b",
        r"\bposted a\s+\d+:\d+\s+k:bb\b",
    ]
    return any(re.search(p, text) for p in patterns)


def is_retirement_post(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    retirement_terms = [
        "announced his retirement", "announced her retirement", "retired from professional baseball",
        "retirement from professional baseball", "calling it a career", "ended his playing career",
        "ended her playing career", "announced his decision to retire", "announced her decision to retire",
        "is retiring", "retire after", "retire from baseball", "calling it quits"
    ]
    return any(t in text for t in retirement_terms)


def is_low_value_transaction(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    low_value_terms = [
        "minor-league deal", "minor league deal", "non-roster invite", "nri", "camp invite",
        "organizational depth", "depth move", "cash considerations", "player to be named later"
    ]
    return any(t in text for t in low_value_terms)


def has_closer_change_terms(text: str) -> bool:
    norm = normalize_text(text)
    closer_change_terms = [
        "closer", "save chances", "ninth inning", "will handle saves", "in line for saves",
        "moving into the closer role", "lost the closer role", "removed from the closer role",
        "demoted from the closer role", "expected to close", "favorite for saves",
    ]
    return any(term in norm for term in closer_change_terms)


def is_statline_recap(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    recap_patterns = [
        r"\bwent\s+\d+-for-\d+\b",
        r"\bwith\s+\d+\s+strikeouts?\b",
        r"\btossed\s+(?:a|an)\s+perfect\s+inning\b",
        r"\bworked\s+(?:a|an)\s+scoreless\s+(?:inning|frame)\b",
        r"\ballowed\s+\d+\s+(?:earned\s+)?runs?\b",
        r"\bon\s+\d+\s+hits?\b",
        r"\bwhile\s+striking\s+out\s+\d+\b",
        r"\bstruck\s+out\s+\d+\b",
        r"\bover\s+\d+(?:\.\d+)?\s+innings\b",
        r"\bposted\s+a\s+\d+:\d+\s+k:bb\b",
        r"\bscoreless\s+inning(?:s)?\b",
        r"\btook\s+the\s+loss\b",
        r"\bpicked\s+up\s+the\s+win\b",
        r"\bloss\s+to\s+the\b",
        r"\bwin\s+over\s+the\b",
    ]
    return any(re.search(pattern, text) for pattern in recap_patterns)


def is_actionable_role_post(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    if any(term in text for term in ["out of the bullpen", "in the bullpen", "bullpen role", "relief appearance", "in relief"]):
        return False
    actionable_role_terms = [
        "projected by mlb.com to start", "projected to start", "bat cleanup", "bat leadoff",
        "hit cleanup", "hit leadoff", "starting pitcher for thursday's season opener",
        "season opener", "opening day starter", "opening day lineup", "will start at",
        "starting at first base", "starting at third base", "starting in center field",
    ]
    return any(term in text for term in actionable_role_terms)


def is_actionable_transaction_post(player_name: str, update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    if _subject_direct_movement(player_name, update_text):
        return True
    transaction_terms = [
        "optioned", "reassigned", "designated for assignment", "signed", "released",
        "waived", "claimed", "traded", "acquired"
    ]
    return any(term in text for term in transaction_terms)


def should_hard_skip_item(player_name: str, category: str, update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)

    if is_retirement_post(update_text, details_text):
        return True

    if category == "injury" and looks_like_actionable_injury_news(text):
        if not is_pitcher_box_score_recap(update_text, details_text) and not is_hitter_box_score_recap(update_text, details_text):
            return False

    if is_statline_recap(update_text, details_text):
        if not _subject_direct_movement(player_name, update_text) and not _subject_roster_outcome(player_name, update_text):
            if not looks_like_actionable_injury_news(text):
                if not is_actionable_transaction_post(player_name, update_text, details_text):
                    if not is_actionable_role_post(update_text, details_text):
                        return True

    bullpen_usage_terms = [
        "plans to deploy", "out of the bullpen", "in the bullpen", "bullpen role",
        "relief appearance", "in relief", "inning of relief", "tossed a perfect inning of relief",
        "worked a scoreless inning", "worked a scoreless frame",
    ]
    if any(term in text for term in bullpen_usage_terms):
        if not has_closer_change_terms(text) and not looks_like_actionable_injury_news(text) and not _subject_direct_movement(player_name, update_text):
            if not _subject_roster_outcome(player_name, update_text):
                return True

    return False


def is_performance_post(text: str) -> bool:
    norm = normalize_text(text)
    return any(re.search(p, norm, re.I) for p in PERFORMANCE_PATTERNS)


def is_obvious_performance_only(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    performance_terms = [
        " batting order ", " slotted fifth ", " in the batting order ", " two games since he returned ",
        " diminished velocity ", " grapefruit league game ", " spring training game ",
        " exhibition against ", " exhibition game ", " world baseball classic ",
        " in the two games since ", " velocity during ",
    ]
    health_or_roster_terms = [
        " injured list", " rehab assignment", " opening day roster", " roster spot",
        " optioned", " reassigned", " signed", " dfa", " designated for assignment",
        " expected to begin the regular season", " oblique", " shoulder", " arm", " hamstring",
        " illness", " mri", " healing well", " relief appearance",
    ]
    if any(term in text for term in health_or_roster_terms):
        return False
    return any(term in text for term in performance_terms)


def classify_item(update_text: str, details_text: str = "") -> str:
    norm = f" {normalize_text(update_text + ' ' + details_text)} "

    lineup_terms = [
        " opening day roster ", " roster spot ", " expected to make the opening day roster ",
        " expected to earn a roster spot ", " will not be a part of the opening day roster ",
        " secured a spot ", " will make the ", " earned a roster spot ",
        " expected to make the roster ", " part of the opening day roster ",
        " included on the opening day roster ", " included on cleveland's opening day roster ",
        " expected to be included on the opening day roster ", " make the club's opening day roster ",
        " expected to be part of the opening day roster ", " expected to make cleveland's opening day roster ",
        " expected to make the guardians' opening day roster ",
    ]
    direct_transaction_terms = [
        " was optioned ", " was reassigned ", " was designated for assignment ",
        " optioned to ", " reassigned to ", " designated for assignment ",
        " was claimed ", " was released ", " was waived ", " was assigned to ",
        " signed a ", " agreed to a ", " was traded ", " was acquired ",
    ]
    broad_transaction_terms = [
        " optioned ", " reassigned ", " designated for assignment ", " dfa ", " signed ",
        " agreed to a ", " claimed ", " released ", " waived ", " selected the contract ",
        " assigned to ", " traded ", " acquired ",
    ]
    role_terms = [
        " will play third base ", " playing time at third base ", " see most of his playing time at ",
        " regular playing time at ", " regular work at ", " primary ", " expected to split time ",
        " expected to see time at ", " expected to work at ", " fill the vacancy ", " more starts ",
        " rotation ", " starter ", " long reliever ", " closer ", " setup role ",
        " bullpen role ", " fifth starter ", " will start ",
        " batting order ", " slotted ", " hit fifth ", " hit cleanup ", " bat leadoff ",
    ]
    injury_terms = [
        " injured list ", " il ", " rehab assignment ", " rehab ", " fracture ", " x-rays ",
        " expected to begin the regular season on the injured list ", " mri ", " day to day ",
        " scratched from ", " healing well ", " relief appearance ", " illness ",
        " strain ", " sprain ", " oblique ",
    ]

    if any(term in norm for term in lineup_terms):
        if any(term in norm for term in direct_transaction_terms):
            return "transaction"
        return "lineup"

    if any(term in norm for term in role_terms):
        return "role"

    if any(term in norm for term in injury_terms):
        return "injury"

    if any(term in norm for term in broad_transaction_terms):
        return "transaction"

    return "general"



def _player_name_forms(player_name: str) -> List[str]:
    player_name = clean_spaces(player_name)
    if not player_name:
        return []
    forms = [player_name.lower()]
    parts = player_name.split()
    if parts:
        forms.append(parts[-1].lower())
    return list(dict.fromkeys(forms))


def _subject_direct_movement(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    for form in _player_name_forms(player_name):
        patterns = [
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?optioned\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?reassigned\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?designated for assignment\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?claimed\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?released\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?waived\b",
            rf"\b{re.escape(form)}\b\s+(?:agreed|signed)\b",
            rf"\boptioned\b.*?\b{re.escape(form)}\b",
            rf"\breassigned\b.*?\b{re.escape(form)}\b",
            rf"\bdesignated\b.*?\b{re.escape(form)}\b\s+for assignment\b",
            rf"\bclaimed\b.*?\b{re.escape(form)}\b",
            rf"\breleased\b.*?\b{re.escape(form)}\b",
            rf"\bwaived\b.*?\b{re.escape(form)}\b",
            rf"\bsigned\b.*?\b{re.escape(form)}\b",
            rf"\bacquired\b.*?\b{re.escape(form)}\b",
            rf"\btraded\b.*?\b{re.escape(form)}\b",
        ]
        if any(re.search(p, text) for p in patterns):
            return True
    return False


def _subject_roster_outcome(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    roster_phrases = [
        "opening day roster", "roster spot", "expected to make the roster",
        "expected to earn a roster spot", "included on the opening day roster",
        "expected to be included on the opening day roster", "expected to be part of the opening day roster",
        "earned a roster spot", "will make the", "secured a spot",
        "make the club's opening day roster", "will be included on",
        "expected to make the opening day roster",
    ]
    return any(p in text for p in roster_phrases)


def _subject_role_usage(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    role_phrases = [
        "see most of his playing time at", "will play third base", "playing time at third base",
        "regular playing time at", "regular work at", "fill the vacancy", "more starts",
        "expected to see time at", "expected to work at", "split time",
        "plans to deploy", "out of the bullpen", "in the bullpen", "opening day rotation",
        "slot in to the dodgers' opening day rotation", "will open the season in the cardinals' rotation",
        "in line for a starting role", "starting role to begin the season",
        "line for regular work", "projected to start", "projected by mlb.com to start",
        "bench role", "platoon role", "playing time at third", "at third base this season",
    ]
    return any(p in text for p in role_phrases)


def _subject_injury_availability(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    injury_phrases = [
        "injured list", "rehab assignment", "expected to begin the regular season on the injured list",
        "mri", "x-rays", "fracture", "illness", "day to day", "healing well",
        "strain", "sprain", "oblique", "shoulder", "hamstring",
        "thumb", "wrist", "elbow", "knee", "back", "groin", "forearm", "blister",
        "soreness", "tightness", "discomfort", "inflammation",
    ]
    deployment_only_phrases = [
        "plans to deploy", "out of the bullpen", "in the bullpen", "opening day rotation",
        "fill the vacancy", "more starts", "projected to start", "projected by mlb.com to start",
        "bench role", "platoon role", "playing time at third base", "at third base this season",
    ]
    if any(p in text for p in deployment_only_phrases) and not any(p in text for p in injury_phrases):
        return False
    return any(p in text for p in injury_phrases)


def refine_category_for_subject(player_name: str, update_text: str, category: str) -> str:
    if _subject_direct_movement(player_name, update_text):
        return "transaction"
    if _subject_roster_outcome(player_name, update_text):
        return "lineup"
    if _subject_injury_availability(player_name, update_text):
        return "injury"
    if _subject_role_usage(player_name, update_text):
        return "role"
    return category


def is_hitter_box_score_recap(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)

    # Injury/availability supersedes the stat-line filter.
    injury_override_terms = [
        " injured list", " rehab assignment", " expected to begin the regular season on the injured list",
        " mri", " x-rays", " fracture", " illness", " day to day", " healing well",
        " strain", " sprain", " oblique", " shoulder", " hamstring", " thumb", " wrist",
        " elbow", " knee", " back", " groin", " forearm", " blister", " soreness",
        " tightness", " discomfort", " inflammation",
    ]
    if any(term in text for term in injury_override_terms):
        return False

    spring_context_terms = [
        "grapefruit league", "cactus league", "spring training", "exhibition",
    ]
    if not any(term in text for term in spring_context_terms):
        return False

    hitter_patterns = [
        r"\bwent\s+\d+-for-\d+\b",
        r"\bwith\s+\d+\s+strikeouts?\b",
        r"\bwith\s+an?\s+double\b",
        r"\bwith\s+an?\s+triple\b",
        r"\bwith\s+an?\s+home run\b",
        r"\bwith\s+two\s+home runs\b",
        r"\bwith\s+an?\s+rbi\b",
        r"\bwith\s+\d+\s+rbi\b",
        r"\bhit a home run\b",
        r"\bhomered\b",
        r"\bdoubled\b",
        r"\btripled\b",
        r"\bsingled\b",
        r"\bdrove in\b",
        r"\bstole a base\b",
    ]
    return any(re.search(pattern, text) for pattern in hitter_patterns)

def should_skip_low_priority(category: str, update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    update_norm = normalize_text(update_text)

    if is_retirement_post(update_text, details_text):
        return True

    if is_pitcher_box_score_recap(update_text, details_text):
        return True

    if is_hitter_box_score_recap(update_text, details_text):
        return True

    if is_performance_post(update_text) or is_obvious_performance_only(update_text, details_text):
        return True

    ripple_terms = [
        " because strider ", " due to strider ",
        " diminished velocity during sunday's grapefruit league game ",
    ]
    if any(term in text for term in ripple_terms):
        return True

    clear_roster_terms = [
        " opening day roster", " roster spot", " expected to make the roster",
        " expected to earn a roster spot", " included on the opening day roster",
        " expected to be included on the opening day roster", " expected to be part of the opening day roster",
        " will not be a part of the opening day roster", " earned a roster spot",
    ]
    clear_movement_terms = [
        " was optioned", " was reassigned", " was designated for assignment",
        " optioned to", " reassigned to", " designated for assignment",
        " signed a", " agreed to a", " was claimed", " was released", " was waived",
        " was traded", " was acquired",
    ]
    clear_injury_terms = [
        " injured list", " rehab assignment", " expected to begin the regular season on the injured list",
        " mri", " x-rays", " fracture", " illness", " day to day", " healing well",
        " strain", " sprain", " oblique", " tightness", " discomfort", " inflammation",
    ]
    clear_role_terms = [
        " see most of his playing time at", " will play third base", " bench role", " platoon role",
        " playing time at third base", " at third base this season",
    ]

    if any(term in text for term in clear_roster_terms):
        return False
    if any(term in text for term in clear_movement_terms):
        return False
    if any(term in text for term in clear_injury_terms):
        return False
    if any(term in text for term in clear_role_terms):
        return False

    if category in {"transaction", "injury", "lineup", "role"}:
        return False

    hard_box_score_terms = [
        " took the loss ", " took a loss ", " scoreless inning", " scoreless innings",
        " allowing one earned run ", " allowing two earned runs ", " allowing three earned runs ",
        " allowed one run ", " allowed two runs ", " allowed three runs ", " allowed four runs ",
        " on one hit ", " on two hits ", " on three hits ", " on four hits ",
        " including one home run ", " including two home runs ",
        " struck out ", " strikeouts ", " k:bb ", " exhibition against ", " exhibition game ", " exhibition defeat ",
        " grapefruit league ", " spring training game ", " through three appearances ",
        " over three innings ", " over two innings ", " over four innings ",
        " one earned run on two hits", " one home run",
    ]
    if any(term in update_norm for term in hard_box_score_terms):
        return True

    return True


def should_skip_rp_blurb(player_name: str, team_hint: Optional[str], category: str, update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)

    rp_usage_or_assignment_terms = [
        "out of the bullpen", "in the bullpen", "reliever", "relief role",
        "setup role", "bullpen role", "middle relief", "seventh inning", "eighth inning",
        "late-inning", "high-leverage", "leverage role", "relief appearance",
        "in relief", "worked a scoreless inning", "worked a scoreless frame",
        "tossed a perfect inning of relief", "bullpen",
        "part of the team's bullpen", "part of the bullpen", "to begin the season in the bullpen",
        "to begin the season out of the bullpen", "will be part of the team's bullpen to begin the season",
    ]

    if any(term in text for term in rp_usage_or_assignment_terms):
        bullpen_assignment_terms = [
            "part of the team's bullpen", "part of the bullpen", "to begin the season in the bullpen",
            "to begin the season out of the bullpen", "will be part of the team's bullpen to begin the season",
            "will open the season in the bullpen", "opening the season in the bullpen",
            "begin the year in the bullpen", "opens the season in the bullpen",
        ]
        if any(term in text for term in bullpen_assignment_terms):
            return True
        if is_pitcher_box_score_recap(update_text, details_text) or is_statline_recap(update_text, details_text):
            return True
        if has_closer_change_terms(text):
            return False
        if looks_like_actionable_injury_news(text):
            injury_only_terms = [
                "injured list", "rehab assignment", "placed on", "expected to begin the regular season on the injured list",
                "will open the season on the injured list", "shut down", "underwent", "surgery", "mri", "x-rays",
                "fracture", "strain", "sprain", "tear", "day to day", "setback", "timeline", "timetable",
                "diagnosed with", "scratched from", "removed from", "tightness", "discomfort", "inflammation",
            ]
            if any(term in text for term in injury_only_terms):
                return False
        return True

    if looks_like_actionable_injury_news(text):
        return False

    if has_closer_change_terms(text):
        return False

    rp_terms = [
        "out of the bullpen", "in the bullpen", "reliever", "relief role",
        "setup role", "bullpen role", "middle relief", "seventh inning", "eighth inning",
        "late-inning", "high-leverage", "leverage role", "relief appearance",
        "in relief", "worked a scoreless inning", "worked a scoreless frame",
        "tossed a perfect inning of relief", "bullpen",
    ]
    pos_terms = [" rp ", " relief pitcher ", " reliever "]

    if any(term in text for term in rp_terms) or any(term in text for term in pos_terms):
        return True

    return False


FANTASY_IMPACT_TEMPLATES = {
    "injury": [
        "The key question here is how much early-season time this could cost him.",
        "This mostly affects availability, so the timetable matters more than the headline.",
        "Treat this as a playing-time hit first and a talent question second.",
        "The fantasy value stays tied to how quickly he can get back on the field.",
        "This is mainly an availability story, so watch for the next timetable update.",
    ],
    "transaction": [
        "This looks like a depth move unless it opens a clearer path to playing time.",
        "There is not much immediate fantasy juice here unless the role grows quickly.",
        "For now, this is more about roster depth than mixed-league value.",
        "The move matters most if it turns into real innings or plate appearances.",
        "This is worth filing away, but the fantasy impact depends on whether the role expands.",
    ],
    "lineup": [
        "Lineup placement can matter quickly, especially in deeper formats and daily leagues.",
        "This is the kind of usage note that can create short-term value if it sticks.",
        "The fantasy appeal rises if this lineup spot holds for more than a few games.",
        "This matters most for managers chasing early playing time and lineup volume.",
        "A favorable lineup role can boost short-term value even before the skills change.",
    ],
    "role": [
        "The fantasy impact comes down to whether this usage pattern becomes the norm.",
        "This role note is worth tracking because it can shift short-term opportunity fast.",
        "There is more fantasy relevance here if the workload or lineup spot stays in place.",
        "Usage clarity matters, and this update may point to where the team is leaning.",
        "This becomes more actionable if the same role shows up again over the next few days.",
    ],
    "other": [
        "This is more of a watch-list update until the next piece of news fills in the picture.",
        "The takeaway here is to monitor the follow-up rather than rush into a move.",
        "There is some fantasy relevance here, but the next update will matter more.",
        "This is useful context, though the impact depends on what happens next.",
        "For now, it is mainly a note to keep on the radar rather than act on immediately.",
    ],
}




def _trim_to_sentence_or_word(text: str, max_len: int) -> str:
    text = clean_spaces(text)
    if len(text) <= max_len:
        return text
    clipped = text[:max_len].rstrip()
    sentence_matches = list(re.finditer(r"[.!?](?=\s|$)", clipped))
    if sentence_matches:
        end = sentence_matches[-1].end()
        candidate = clipped[:end].strip()
        if candidate:
            return candidate
    word_cut = clipped.rsplit(" ", 1)[0].strip()
    return word_cut or clipped

def choose_varied_template(options: List[str], seed_text: str) -> str:
    if not options:
        return "Watch for the next update before reacting."
    idx = int(hashlib.sha1(seed_text.encode("utf-8")).hexdigest(), 16) % len(options)
    return options[idx]


def summarize_fantasy_impact(category: str, update_text: str, details_text: str) -> str:
    category_key = category if category in FANTASY_IMPACT_TEMPLATES else "other"
    options = FANTASY_IMPACT_TEMPLATES.get(category_key, FANTASY_IMPACT_TEMPLATES["other"])
    return choose_varied_template(options, f"{category}|{update_text}|{details_text}")


def rewrite_update_blurb(player_name: str, category: str, update_text: str, details_text: str, team_abbr: str = "") -> str:
    text = strip_reporter_tail(clean_spaces(update_text))
    if not text:
        return clean_spaces(update_text)

    norm = normalize_text(text)
    team_label = TEAM_CITY_BY_ABBR.get(str(team_abbr or "").upper(), str(team_abbr or "").upper())
    last_name = clean_spaces(player_name).split()[-1] if clean_spaces(player_name) else "Player"

    injury_patterns = [
        (r"\bplaced (?:him )?on the ([0-9]+)-day injured list\b", lambda m: f"{player_name} was placed on the {m.group(1)}-day IL."),
        (r"\bplaced (?:him )?on the 60-day injured list\b", lambda m: f"{player_name} was moved to the 60-day IL."),
        (r"\bwill begin the season on the (?:injured list|il)\b", lambda m: f"{player_name} is set to open the season on the IL."),
        (r"\bis scheduled to begin a rehab assignment(?:\s+([A-Za-z]+))?\b", lambda m: f"{player_name} is set to begin a rehab assignment{" " + m.group(1) if m.group(1) else ""}.".replace(" .", ".")),
        (r"\bbegan a rehab assignment\b|\bbegins a rehab assignment\b", lambda m: f"{player_name} has started a rehab assignment."),
        (r"\breturns? to the lineup\b", lambda m: f"{player_name} is back in the lineup."),
    ]
    for pattern, builder in injury_patterns:
        m = re.search(pattern, norm, re.I)
        if m:
            return clean_spaces(builder(m))

    transaction_patterns = [
        (r"\bsigned\b.*\bcontract\b", lambda m: f"{team_label} signed {player_name}." if team_label else f"{player_name} signed a new deal."),
        (r"\bselected the contract of\b", lambda m: f"{team_label} selected {player_name}'s contract." if team_label else f"{player_name}'s contract was selected."),
        (r"\brecalled\b", lambda m: f"{team_label} recalled {player_name}." if team_label else f"{player_name} was recalled."),
        (r"\boptioned\b", lambda m: f"{player_name} was optioned to the minors."),
        (r"\breassigned\b", lambda m: f"{player_name} was reassigned to minor-league camp."),
        (r"\bdesignated for assignment\b", lambda m: f"{player_name} was designated for assignment."),
    ]
    if category == "transaction":
        for pattern, builder in transaction_patterns:
            if re.search(pattern, norm, re.I):
                return clean_spaces(builder(None))

    role_patterns = [
        (r"\bcloser job\b.*\bdepend on the situation\b", lambda m: f"{team_label} appear set to use a closer committee." if team_label else "The team appears set to use a closer committee."),
        (r"\bcomfortable with\b.*\bcloser role\b", lambda m: f"{team_label} appear set to use a closer committee." if team_label else "The team appears set to use a closer committee."),
        (r"\bprojected by mlb\.com to start at ([a-z ]+) and bat ([a-z0-9-]+)\b", lambda m: f"{player_name} is projected to start at {m.group(1)} and bat {m.group(2)}."),
        (r"\bprojected to start at ([a-z ]+) and bat ([a-z0-9-]+)\b", lambda m: f"{player_name} is projected to start at {m.group(1)} and bat {m.group(2)}."),
        (r"\bwill bat cleanup\b", lambda m: f"{player_name} is expected to bat cleanup."),
        (r"\bwill bat leadoff\b", lambda m: f"{player_name} is expected to hit leadoff."),
    ]
    if category == "role":
        for pattern, builder in role_patterns:
            m = re.search(pattern, norm, re.I)
            if m:
                sentence = builder(m)
                for long_pos, short_pos in POSITION_HEADLINE_MAP.items():
                    sentence = re.sub(rf"\b{re.escape(long_pos)}\b", short_pos, sentence, flags=re.I)
                return clean_spaces(sentence)

    sentence = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0].strip()
    sentence = re.sub(r"^[A-Z][a-z]+ manager [A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)* said [A-Za-z]+ that ", "", sentence)
    sentence = re.sub(r"^[A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)* said [A-Za-z]+ that ", "", sentence)
    sentence = re.sub(r",?\s*[A-Z][a-z]+day\b", "", sentence)
    sentence = re.sub(rf"^{re.escape(last_name)}(?:'s)?\s+", "", sentence, flags=re.I)
    sentence = re.sub(r"\((?:[^()]*)\)", "", sentence)
    sentence = re.sub(r"\s+", " ", sentence).strip(" .,-")
    if not sentence:
        sentence = text
    if player_name and not sentence.lower().startswith(player_name.lower()):
        sentence = f"{player_name} {sentence[:1].lower() + sentence[1:] if sentence else ''}".strip()
    if sentence and not sentence.endswith('.'):
        sentence += '.'
    return clean_spaces(sentence)


POSITION_HEADLINE_MAP = {
    "first base": "1B",
    "second base": "2B",
    "third base": "3B",
    "shortstop": "SS",
    "left field": "LF",
    "center field": "CF",
    "right field": "RF",
    "outfield": "OF",
    "catcher": "C",
    "designated hitter": "DH",
    "starting pitcher": "SP",
    "relief pitcher": "RP",
    "pitcher": "P",
}

TEAM_CITY_BY_ABBR = {
    "ARI": "Arizona",
    "ATH": "Athletics",
    "ATL": "Atlanta",
    "BAL": "Baltimore",
    "BOS": "Boston",
    "CHC": "Cubs",
    "CWS": "White Sox",
    "CIN": "Reds",
    "CLE": "Guardians",
    "COL": "Colorado",
    "DET": "Detroit",
    "HOU": "Houston",
    "KC": "Royals",
    "LAA": "Angels",
    "LAD": "Dodgers",
    "MIA": "Marlins",
    "MIL": "Brewers",
    "MIN": "Twins",
    "NYM": "Mets",
    "NYY": "Yankees",
    "PHI": "Phillies",
    "PIT": "Pirates",
    "SD": "Padres",
    "SF": "Giants",
    "SEA": "Mariners",
    "STL": "Cardinals",
    "TB": "Rays",
    "TEX": "Rangers",
    "TOR": "Blue Jays",
    "WSH": "Nationals",
}


def shorten_headline_text(text: str, max_len: int = 42) -> str:
    text = clean_spaces(text).strip(" .,-")
    if len(text) <= max_len:
        return text
    cutoff = text[:max_len].rstrip()
    if " " in cutoff:
        cutoff = cutoff.rsplit(" ", 1)[0]
    cutoff = cutoff.rstrip(" ,.-")
    return cutoff or text[:max_len].rstrip(" ,.-")


def _headline_team_name(team_abbr: str, sentence: str) -> str:
    sentence = clean_spaces(sentence)
    first = sentence.split()[0] if sentence else ""
    if re.fullmatch(r"[A-Z][A-Za-z'.-]+", first):
        return first
    return TEAM_CITY_BY_ABBR.get(str(team_abbr or "").upper(), str(team_abbr or "MLB").upper())


def make_subject_headline(player_name: str, update_text: str, team_abbr: str = "", category: str = "") -> str:
    text = strip_reporter_tail(clean_spaces(update_text))
    if not text:
        return shorten_headline_text(player_name)

    sentence = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0].strip()
    norm = normalize_text(sentence)
    player_clean = clean_spaces(player_name)
    last_name = player_clean.split()[-1] if player_clean else "Player"
    team_name = _headline_team_name(team_abbr, sentence)
    team_label = TEAM_CITY_BY_ABBR.get(str(team_abbr or "").upper(), team_name)

    if re.search(r"\bcloser job\b", norm) and re.search(r"depend on the situation|committee", norm):
        return shorten_headline_text(f"{team_label} go closer by committee")
    if re.search(r"\bcomfortable with\b", norm) and re.search(r"\bcloser role\b|\bin the role\b", norm):
        return shorten_headline_text(f"{team_label} go closer by committee")

    injury_patterns = [
        (r"\bplaced on the ([0-9]+)-day injured list\b", r"Placed on \1-day IL"),
        (r"\bplaced him on the ([0-9]+)-day injured list\b", r"Placed on \1-day IL"),
        (r"\bplaced on the 60-day injured list\b", "Moved to 60-day IL"),
        (r"\bplaced him on the 60-day injured list\b", "Moved to 60-day IL"),
        (r"\bwill begin the season on the (?:injured list|il)\b", "Begins season on IL"),
        (r"\bexpected to begin the season on the (?:injured list|il)\b", "Expected to open season on IL"),
        (r"\bplaced on the injured list\b", "Placed on IL"),
        (r"\breturns? to the lineup\b", "Returns to lineup"),
        (r"\bbegan a rehab assignment\b", "Begins rehab assignment"),
        (r"\bbegins a rehab assignment\b", "Begins rehab assignment"),
        (r"\bis scheduled to begin a rehab assignment\b", "Begins rehab assignment Saturday"),
        (r"\btook batting practice\b", "Takes batting practice"),
        (r"\bis scheduled to play catch\b", "Set to resume playing catch"),
    ]
    for pattern, repl in injury_patterns:
        m = re.search(pattern, norm, re.I)
        if m:
            return shorten_headline_text(m.expand(repl) if "\\1" in repl else repl)

    transaction_templates = [
        (rf"^(?:the\s+)?{re.escape(team_name.lower())}\s+signed\b", f"{team_name} signs {last_name}"),
        (r"\bsigned\b.*\bminor-league contract\b", f"{team_name} signs {last_name}"),
        (r"\bsigned\b.*\bcontract\b", f"{team_name} signs {last_name}"),
        (r"\bselected the contract of\b", f"{team_name} selects {last_name}"),
        (r"\brecalled\b", f"{team_name} recalls {last_name}"),
        (r"\boptioned\b", "Optioned to minors"),
        (r"\breassigned\b", "Reassigned to camp"),
        (r"\bdesignated for assignment\b", "Designated for assignment"),
        (r"\bclaimed\b", "Claimed off waivers"),
        (r"\btraded\b", "Traded to new club"),
        (r"\bacquired\b", "Acquired in trade"),
    ]
    for pattern, headline in transaction_templates:
        if re.search(pattern, norm, re.I):
            return shorten_headline_text(headline)

    role_patterns = [
        (r"\bis projected by mlb\.com to start at ([a-z ]+) and bat ([a-z0-9-]+)\b", r"Projected to start at \1, bat \2"),
        (r"\bis projected to start at ([a-z ]+) and bat ([a-z0-9-]+)\b", r"Projected to start at \1, bat \2"),
        (r"\bwill start at ([a-z ]+)\b", r"Starting at \1"),
        (r"\bis expected to start at ([a-z ]+)\b", r"Expected to start at \1"),
        (r"\bhas made the .* opening day rotation\b", "Makes Opening Day rotation"),
        (r"\bwill bat cleanup\b", "Set to bat cleanup"),
        (r"\bwill bat leadoff\b", "Set to bat leadoff"),
    ]
    for pattern, repl in role_patterns:
        m = re.search(pattern, norm, re.I)
        if m:
            headline = m.expand(repl)
            headline = headline.split(" against ")[0]
            for long_pos, short_pos in POSITION_HEADLINE_MAP.items():
                headline = re.sub(rf"\b{re.escape(long_pos)}\b", short_pos, headline, flags=re.I)
            headline = headline[:1].upper() + headline[1:]
            return shorten_headline_text(headline)

    sentence = re.sub(r"^[A-Z][a-z]+ manager [A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)* said [A-Za-z]+ that ", "", sentence)
    sentence = re.sub(r"^[A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)* said [A-Za-z]+ that ", "", sentence)
    sentence = re.sub(rf"^{re.escape(player_clean)}(?:'s)?\s+", "", sentence, flags=re.I)
    sentence = re.sub(r"\((?:[^()]*)\)", "", sentence)

    replacements = [
        (r"\bis projected by MLB\.com to\b", "Projected to"),
        (r"\bis projected to\b", "Projected to"),
        (r"\bhas been projected to\b", "Projected to"),
        (r"\bis expected to\b", "Expected to"),
        (r"\bis slated to\b", "Set to"),
        (r"\bis set to\b", "Set to"),
        (r"\bwill open the season\b", "Opens season"),
        (r"\bwill begin the season\b", "Begins season"),
        (r"\bwill begin\b", "Begins"),
        (r"\bwill start at\b", "Starting at"),
    ]
    for pattern, repl in replacements:
        sentence = re.sub(pattern, repl, sentence, flags=re.I)

    for long_pos, short_pos in POSITION_HEADLINE_MAP.items():
        sentence = re.sub(rf"\b{re.escape(long_pos)}\b", short_pos, sentence, flags=re.I)

    sentence = re.sub(r"\bagainst\b.*$", "", sentence, flags=re.I)
    sentence = re.sub(r",\s*(but|with|while|after)\b.*$", "", sentence, flags=re.I)
    sentence = re.sub(r"\s+", " ", sentence).strip(" .,-")
    if not sentence:
        sentence = clean_spaces(update_text)
    if sentence:
        sentence = sentence[:1].upper() + sentence[1:]
    return shorten_headline_text(sentence)


def canonical_story_key(player_name: str, update_text: str, details_text: str) -> str:
    return sha1_text(f"{player_name.lower()}|{normalize_text(strip_reporter_tail(update_text))}|{normalize_text(details_text)}")


def exact_item_key(player_name: str, category: str, update_text: str, details_text: str) -> str:
    return sha1_text(f"{player_name.lower()}|{category}|{normalize_text(update_text)}|{normalize_text(details_text)}")


def _sanitize_player_entry(name: str, entry: Any) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    cleaned_name = clean_spaces(name)
    if not is_valid_player_name(cleaned_name):
        return None, None
    if not isinstance(entry, dict):
        return None, None
    team = str(entry.get("team") or "").upper().strip()
    if team and team not in TEAM_META:
        team = ""
    pos = clean_spaces(str(entry.get("position") or entry.get("pos") or "").upper())
    if pos and pos not in VALID_POSITIONS:
        pos = ""
    cleaned = dict(entry)
    cleaned["team"] = team
    if pos:
        cleaned["position"] = pos
    return cleaned_name, cleaned


def load_player_ids_map() -> Dict[str, Any]:
    source_path = PLAYER_ID_PATH if PLAYER_ID_PATH.exists() else LOCAL_PLAYER_ID_PATH
    if not source_path.exists():
        log(f"Player id map missing at {PLAYER_ID_PATH} and {LOCAL_PLAYER_ID_PATH}")
        return {}
    try:
        raw = json.loads(source_path.read_text(encoding="utf-8"))
        cleaned: Dict[str, Any] = {}
        dropped = 0
        for name, entry in raw.items():
            clean_name, clean_entry = _sanitize_player_entry(name, entry)
            if not clean_name or not clean_entry:
                dropped += 1
                continue
            cleaned[clean_name] = clean_entry
        log(f"Loaded ESPN player id map from {source_path} | kept={len(cleaned)} | dropped={dropped}")
        return cleaned
    except Exception as exc:
        log(f"Failed loading player id map {source_path}: {exc}")
        return {}


def build_last_name_index(player_ids_map: Dict[str, Any]) -> Dict[str, List[str]]:
    index: Dict[str, List[str]] = {}
    for full_name in player_ids_map:
        parts = clean_spaces(full_name).split()
        if parts:
            index.setdefault(parts[-1].lower(), []).append(full_name)
    return index


def build_name_prefixes(player_ids_map: Dict[str, Any]) -> List[str]:
    return sorted(player_ids_map.keys(), key=len, reverse=True)


def resolve_player_name(raw_name: str, player_ids_map: Dict[str, Any], last_name_index: Dict[str, List[str]]) -> str:
    raw_name = clean_spaces(raw_name)
    raw_name = KNOWN_NAME_FIXES.get(raw_name, raw_name)

    if raw_name in player_ids_map:
        return raw_name

    normalized_target = normalize_person_name(raw_name)
    for candidate in player_ids_map.keys():
        if normalize_person_name(candidate) == normalized_target:
            return candidate

    parts = raw_name.split()
    if parts:
        last = parts[-1].lower()
        matches = last_name_index.get(last, [])
        if len(matches) == 1:
            return matches[0]
    return raw_name


def is_suspicious_row_mismatch(player_name: str, team_hint: Optional[str], update_text: str) -> bool:
    text = normalize_text(update_text)
    if player_name == "Kody Funderburk" and "derek shelton" in text:
        return True
    if team_hint == "MIN" and "derek shelton" in text:
        return True
    if player_name == "JP Sears" and "el paso" in text:
        return True
    if player_name == "Jose Suarez" and "strider" in text:
        return True
    if re.search(r"(Red|White|Pirates|Padres|Giants|Mariners|Diamondbacks|Twins|Yankees|Dodgers|Mets|Orioles|Cubs|Cardinals|Athletics|Rays|Royals|Rockies|Brewers|Braves|Nationals|Tigers|Rangers|Astros|Phillies|Marlins|Angels)(?:RP|SP|DH|OF|P)?$", player_name):
        return True
    return False


def resolve_player_card_assets(
    player_name: str,
    hinted_team: Optional[str],
    player_ids_map: Dict[str, Any],
    last_name_index: Dict[str, List[str]],
) -> Dict[str, Any]:
    resolved_name = resolve_player_name(player_name, player_ids_map, last_name_index)
    entry = player_ids_map.get(resolved_name) if isinstance(player_ids_map.get(resolved_name), dict) else None
    team_abbr = str(hinted_team or "").upper()
    headshot_url = None
    position = ""

    if entry:
        team_abbr = str(entry.get("team") or team_abbr or "").upper()
        headshot_url = entry.get("headshot_url")
        position = clean_spaces(str(entry.get("position") or "").upper())

    if team_abbr in TEAM_META:
        team_meta = TEAM_META[team_abbr]
        team_logo_url = f"https://a.espncdn.com/i/teamlogos/mlb/500/{team_meta['slug']}.png"
        color = team_meta["color"]
    else:
        team_logo_url = MLB_FALLBACK_LOGO
        color = 0x1D4ED8

    return {
        "resolved_name": resolved_name,
        "team_abbr": team_abbr or "MLB",
        "position": position,
        "team_logo_url": team_logo_url,
        "color": color,
        "headshot_url": headshot_url or team_logo_url,
    }


@dataclass
class NewsItem:
    source: str
    source_id: str
    player_name: str
    update_text: str
    details_text: str
    category: str
    published_label: Optional[str] = None
    status: str = "new"
    team_hint: Optional[str] = None

    def exact_hash(self) -> str:
        return exact_item_key(self.player_name, self.category, self.update_text, self.details_text)

    def story_hash(self) -> str:
        return canonical_story_key(self.player_name, self.update_text, self.details_text)


class ESPNSource:
    def __init__(self, player_ids_map: Dict[str, Any]):
        self.player_ids_map = player_ids_map
        self.name_prefixes = build_name_prefixes(player_ids_map)

    def _extract_left_name(self, left: str) -> Optional[str]:
        for candidate in self.name_prefixes:
            if left.startswith(candidate + " "):
                return candidate

        left_lower = " " + left.lower() + " "
        earliest = None
        for team_word in sorted(TEAM_WORD_TO_ABBR.keys(), key=len, reverse=True):
            token = " " + team_word + " "
            pos = left_lower.find(token)
            if pos != -1:
                real_pos = max(0, pos - 1)
                if earliest is None or real_pos < earliest:
                    earliest = real_pos

        candidate = None
        if earliest is not None:
            candidate = clean_spaces(left[:earliest])

        if not candidate:
            m = re.match(r"^([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,2})\b", left)
            if m:
                candidate = clean_spaces(m.group(1))

        if not candidate:
            return None

        # Trim flattened team residue like "MarinersOF", "PadresRP", "White", "Red"
        team_noise_patterns = [
            r"\s+(?:Red|White|Blue)$",
            r"(?:Pirates|Padres|Giants|Mariners|Diamondbacks|Twins|Yankees|Dodgers|Mets|Orioles|Cubs|Cardinals|Athletics|Rays|Royals|Rockies|Brewers|Braves|Nationals|Tigers|Rangers|Astros|Phillies|Marlins|Angels)(?:RP|SP|DH|OF|P)$",
        ]
        for pattern in team_noise_patterns:
            candidate = re.sub(pattern, "", candidate)
        candidate = POS_SUFFIX_RE.sub("", candidate).strip()
        candidate = clean_spaces(candidate)
        return candidate or None

    def _extract_team_hint(self, left: str) -> Optional[str]:
        left_lower = left.lower()

        # First, normal spaced team match
        for team_word, abbr in sorted(TEAM_WORD_TO_ABBR.items(), key=lambda x: len(x[0]), reverse=True):
            if f" {team_word} " in f" {left_lower} ":
                return abbr

        # Fallback for flattened team+pos strings like MarinersOF, PadresRP, DiamondbacksSP
        normalized_left = re.sub(r"\s+", "", left_lower)
        for team_word, abbr in sorted(TEAM_WORD_TO_ABBR.items(), key=lambda x: len(x[0]), reverse=True):
            squashed_team = team_word.replace(" ", "")
            if squashed_team in normalized_left:
                return abbr

        return None

    def _parse_row_text(self, text: str) -> Optional[NewsItem]:
        text = clean_spaces(text)
        if "News Archive" not in text:
            return None

        parts = text.split("News Archive", 1)
        if len(parts) != 2:
            return None

        left = clean_spaces(parts[0])
        right = clean_spaces(parts[1])

        full_name = self._extract_left_name(left)
        if not full_name:
            return None

        full_name = KNOWN_NAME_FIXES.get(full_name, full_name)
        if not is_valid_player_name(full_name):
            return None
        team_hint = self._extract_team_hint(left)

        ts_match = TIMESTAMP_RE.search(right)
        if not ts_match:
            return None

        body = clean_spaces(right[ts_match.end():])
        if "Spin:" in body:
            update_text, details_text = body.split("Spin:", 1)
        else:
            update_text, details_text = body, ""

        update_text = strip_reporter_tail(clean_spaces(update_text))
        details_text = clean_spaces(details_text)

        if not update_text:
            return None

        category = classify_item(update_text, details_text)
        category = refine_category_for_subject(full_name, update_text, category)
        source_id = sha1_text(text[:700])

        return NewsItem(
            source="espn",
            source_id=source_id,
            player_name=full_name,
            update_text=update_text,
            details_text=details_text,
            category=category,
            published_label=None,
            team_hint=team_hint,
        )

    async def fetch_items(self) -> List[NewsItem]:
        if async_playwright is None:
            raise RuntimeError("Playwright is not installed. Run: python3 -m pip install playwright && python3 -m playwright install")

        log(f"Opening {ESPN_URL}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(user_agent=USER_AGENT, viewport={"width": 1440, "height": 2600})
            page = await context.new_page()
            await page.goto(ESPN_URL, wait_until="domcontentloaded", timeout=60000)

            for _ in range(6):
                await page.wait_for_timeout(1500)
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2500)

            row_candidates = await page.evaluate(
                """
                () => {
                  const nodes = Array.from(document.querySelectorAll("article, section, li, div, tr"));
                  const out = [];
                  for (const el of nodes) {
                    const text = (el.innerText || "").replace(/\\s+/g, " ").trim();
                    if (!text) continue;
                    if (text.length < 80 || text.length > 4000) continue;
                    if (!text.includes("News Archive")) continue;
                    out.push(text);
                  }
                  return out;
                }
                """
            )

            await context.close()
            await browser.close()

        unique_rows: List[str] = []
        seen: Set[str] = set()
        for text in row_candidates:
            text = clean_spaces(text)
            if text and text not in seen and not looks_like_nav_or_shell(text):
                seen.add(text)
                unique_rows.append(text)

        debug_lines = ["ROW CANDIDATES", "=" * 60]
        for i, text in enumerate(unique_rows[:300], start=1):
            debug_lines.append(f"[{i}] {text}")
            debug_lines.append("")
        write_text_file(SCRAPE_DEBUG_FILE, "\n".join(debug_lines))

        items: List[NewsItem] = []
        for text in unique_rows:
            item = self._parse_row_text(text)
            if item:
                items.append(item)

        deduped: List[NewsItem] = []
        seen_hashes: Set[str] = set()
        for item in items:
            h = item.exact_hash()
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            deduped.append(item)

        log(f"Row candidates={len(unique_rows)} | parsed_items={len(deduped)}")
        return deduped[:100]


class BotState:
    def __init__(self) -> None:
        ensure_state_dir()
        if RESET_STATE_ON_START:
            self.reset()
        self.posted_ids: List[str] = load_json_file(POSTED_IDS_FILE, [])
        self.recent_hashes: Dict[str, str] = load_json_file(RECENT_HASHES_FILE, {})
        self.player_last_posts: Dict[str, Dict[str, Any]] = load_json_file(PLAYER_LAST_POSTS_FILE, {})

    def reset(self) -> None:
        save_json_file(POSTED_IDS_FILE, [])
        save_json_file(RECENT_HASHES_FILE, {})
        save_json_file(PLAYER_LAST_POSTS_FILE, {})
        log("RESET_STATE_ON_START enabled — cleared ESPN state")

    def save(self) -> None:
        save_json_file(POSTED_IDS_FILE, self.posted_ids[-5000:])
        save_json_file(RECENT_HASHES_FILE, self.recent_hashes)
        save_json_file(PLAYER_LAST_POSTS_FILE, self.player_last_posts)

    def seen_exact(self, item: NewsItem) -> bool:
        return item.exact_hash() in self.recent_hashes

    def should_mark_update(self, item: NewsItem) -> bool:
        previous = self.player_last_posts.get(item.player_name.lower())
        if not previous:
            return False
        if previous.get("exact_hash") == item.exact_hash():
            return False
        return previous.get("story_hash") != item.story_hash()

    def record_post(self, item: NewsItem) -> None:
        self.posted_ids.append(item.source_id)
        self.recent_hashes[item.exact_hash()] = datetime.now(timezone.utc).isoformat()
        self.player_last_posts[item.player_name.lower()] = {
            "exact_hash": item.exact_hash(),
            "story_hash": item.story_hash(),
            "update_text": item.update_text,
            "details_text": item.details_text,
            "category": item.category,
            "status": item.status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


class ESPNNewsBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

        self.state = BotState()
        self.player_ids_map = load_player_ids_map()
        self.last_name_index = build_last_name_index(self.player_ids_map)
        self.source = ESPNSource(self.player_ids_map)
        self.current_run_seen: Set[str] = set()

    async def setup_hook(self) -> None:
        self.poll_loop.start()

    async def on_ready(self) -> None:
        log(f"Logged in as {self.user}")
        log(f"Target channel id: {NEWS_CHANNEL_ID}")
        log("Poll loop started")

    def build_embed(self, item: NewsItem) -> discord.Embed:
        assets = resolve_player_card_assets(item.player_name, item.team_hint, self.player_ids_map, self.last_name_index)
        display_name = assets["resolved_name"]
        team_abbr = assets["team_abbr"]
        position = (assets.get("position") or "").upper().strip()
        header_parts = [display_name]
        if position:
            header_parts.append(position)
        if team_abbr:
            header_parts.append(team_abbr)
        header_line = " | ".join(header_parts)

        update_text = clean_spaces(item.update_text)
        details_text = clean_spaces(item.details_text)
        rewritten_update = rewrite_update_blurb(display_name, item.category, update_text, details_text, team_abbr)
        safe_details_text = _trim_to_sentence_or_word(details_text, 1000)

        embed = discord.Embed(
            description=f"**Update:** {rewritten_update}",
            color=assets["color"],
        )
        embed.set_author(name=header_line, icon_url=assets["team_logo_url"])
        embed.set_thumbnail(url=assets["headshot_url"])

        if safe_details_text:
            embed.add_field(name="Details", value=safe_details_text, inline=False)

        embed.add_field(name="​", value="**Source:** [Rotowire](https://www.rotowire.com/baseball/)", inline=False)

        embed.set_footer(text=f"Tag: {item.category.title()}")
        embed.timestamp = datetime.now(timezone.utc)
        return embed

    async def post_item(self, channel: discord.abc.Messageable, item: NewsItem) -> None:
        await channel.send(embed=self.build_embed(item))

    @tasks.loop(minutes=POLL_MINUTES)
    async def poll_loop(self) -> None:
        await self.run_poll_cycle("loop")

    @poll_loop.before_loop
    async def before_poll_loop(self) -> None:
        await self.wait_until_ready()

    async def run_poll_cycle(self, trigger: str = "manual") -> None:
        channel = self.get_channel(NEWS_CHANNEL_ID)
        if channel is None:
            log(f"Channel not found: {NEWS_CHANNEL_ID}")
            return

        log(f"Starting poll cycle | trigger={trigger}")
        self.current_run_seen = set()

        try:
            items = await self.source.fetch_items()
        except Exception as exc:
            log(f"Source fetch failed: {exc}")
            import traceback
            traceback.print_exc()
            return

        log(f"Extracted {len(items)} items")
        posted = 0

        for item in items:
            if posted >= MAX_POSTS_PER_RUN:
                log(f"Reached MAX_POSTS_PER_RUN={MAX_POSTS_PER_RUN}")
                break

            if is_suspicious_row_mismatch(item.player_name, item.team_hint, item.update_text):
                log(f"Skipping suspicious row mismatch: {item.player_name} | {item.update_text[:120]}")
                continue

            if should_hard_skip_item(item.player_name, item.category, item.update_text, item.details_text):
                log(f"Skipping hard-gate item: {item.player_name} | {item.category}")
                continue

            if should_skip_rp_blurb(item.player_name, item.team_hint, item.category, item.update_text, item.details_text):
                log(f"Skipping RP blurb: {item.player_name} | {item.category}")
                continue

            if should_skip_low_priority(item.category, item.update_text, item.details_text):
                log(f"Skipping low-priority item: {item.player_name} | {item.category}")
                continue

            exact_hash = item.exact_hash()
            if exact_hash in self.current_run_seen:
                log(f"Skipping same-run exact duplicate: {item.player_name} | {item.update_text[:120]}")
                continue

            if self.state.seen_exact(item):
                log(f"Skipping posted exact duplicate: {item.player_name} | {item.update_text[:120]}")
                continue

            item.status = "update" if self.state.should_mark_update(item) else "new"
            await self.post_item(channel, item)
            self.current_run_seen.add(exact_hash)
            self.state.record_post(item)
            posted += 1
            log(f"Posted {item.player_name} | {item.category} | {item.status} | {item.update_text[:140]}")

        self.state.save()
        log(f"Poll cycle complete | posted={posted} | found={len(items)}")


def validate_config() -> None:
    if not NEWS_BOT_TOKEN:
        raise RuntimeError("Missing NEWS_BOT_TOKEN environment variable or news_config.NEWS_BOT_TOKEN")
    if not NEWS_CHANNEL_ID:
        raise RuntimeError("Missing NEWS_CHANNEL_ID environment variable or news_config.NEWS_CHANNEL_ID")


def main() -> None:
    validate_config()
    bot = ESPNNewsBot()
    bot.run(NEWS_BOT_TOKEN)


if __name__ == "__main__":
    main()
