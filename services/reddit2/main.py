import os
import re
import time
import yaml
from datetime import datetime, timezone
from typing import Iterable, List, Tuple, Dict, Set, DefaultDict

from tenacity import retry, wait_exponential, stop_after_attempt

import praw
import prawcore
from psycopg import Connection
from psycopg import connect
from psycopg.rows import dict_row

from openai import OpenAI, APIError

from core.config import Settings
from core.logging import info, err


def _mask(v: str | None, keep: int = 6) -> str | None:
    """Utility to mask long secrets when logging (show first 6 chars + ...)."""
    if not v:
        return None
    if len(v) <= keep:
        return v
    return v[:keep] + "…"


def load_subreddits(cfg_path: str, env_fallback: str | None) -> List[str]:
    """Load subreddits list from YAML config file, or fallback to env variable."""
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        subs = data.get("subreddits", [])
        return [s.strip() for s in subs if s and isinstance(s, str)]
    if env_fallback:
        return [s.strip() for s in env_fallback.split(",") if s.strip()]
    return []


# Common English words that are also valid stock tickers. These require
# explicit mention ($ prefix or ALL-CAPS) to count as ticker/alias.
AMBIGUOUS_TICKER_WORDS: Set[str] = set(
    [
        # 1-char
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
        # 2-char
        "ai", "am", "an", "as", "at", "be", "by", "do", "go", "he", "dd", "in", "is", "it", "me", "my", "no", "of","op", "on", "or", "so", "to", "up", "us", "we",
        # 3-char
        "all", "and", "any", "are", "art", "ask", "app","bad", "bar", "bat", "bed", "bet", "big", "bit", "box", "boy", "bug", "bus", "but", "buy", "bro", "can", "car", "cat", "cut", "day", "did", "die", "dog", "eat", "egg", "end", "eye", "far", "fat", "few", "fit", "fly", "for", "fun", "gas", "get", "got", "gun", "guy", "had", "has", "hat", "her", "him", "his", "hit", "hot", "how", "ice", "job", "key", "kid", "law", "let", "lot", "low", "man", "map", "may", "men", "mix", "mom", "mod", "new", "not", "now", "old", "one", "our", "out", "own", "pay", "put", "red", "run", "sad", "say", "sea", "see", "set", "she", "sky", "sun", "ten", "the", "too", "top", "try", "two", "use", "van", "war", "was", "way", "who", "wow", "why", "win","wtf", "yes", "yet", "you", "zip",
        # 4-char common words
        "able", "area", "back", "ball", "bank", "base", "beat", "been", "best", "bird", "blue", "boat", "body", "blsh", "book", "both", "came", "cars", "case", "cash","care", "city", "club", "cold", "come", "cool", "cost", "dark", "data", "date", "days", "deal", "deep", "does", "done", "door", "down", "draw", "drop", "each", "easy", "even", "else", "ever", "face", "fact", "fall", "fast", "fear", "feel", "feet", "fell", "file", "fill", "find", "fine", "fire", "fish", "five", "flat", "flow", "food", "foot", "form", "four", "free", "from", "full","fund", "game", "gain", "gave", "girl", "give", "glad", "goal", "goes", "gold", "gone", "good", "grew", "grew", "guys", "hair", "half", "hand", "hard", "have", "head", "hear", "heat", "held", "help", "here", "high", "hold", "home", "hope", "hour", "huge", "idea", "into", "item", "join", "jump", "just", "keep", "kept", "kind", "kids", "knew", "know", "land", "last", "late", "left", "luck", "life", "link", "like", "line", "list", "live", "long", "look", "lost", "lots", "loud", "love", "made", "mail", "main", "make", "many", "mass", "meal", "mean", "meet", "milk", "mind", "mine", "miss", "mode", "moon", "more", "most", "move", "much", "must", "name", "near", "need", "news", "next", "nice", "nine", "nose", "note", "once", "only", "open", "over", "owns", "page", "paid", "pain", "part", "pass", "past", "path", "plan", "play", "plus", "poor", "port", "post", "push", "quit", "race", "rain", "rang", "read", "real", "rest", "rich", "ride", "ring", "rise", "road", "rock", "room", "rule", "safe", "said", "same", "save", "seat", "seem", "seen", "self", "sell", "send", "sent", "ship", "shop", "show", "shut", "sick", "side", "sign", "site", "size", "skin", "slow", "snow", "soft", "soil", "sold", "some", "song", "soon", "sort", "spot", "stay", "step", "stop", "such", "sure", "take", "talk", "tall", "team", "tell", "term", "test", "text", "than", "that", "them", "then", "they", "this", "thus", "time", "told", "tone", "took", "tool", "town", "tree", "trip", "true", "turn", "type", "unit", "upon", "used", "user", "very", "view", "walk", "wall", "want", "warm", "ways", "wear", "week", "well", "went", "were", "what", "when", "will", "wind", "wire", "wise", "wish", "with", "wood", "word", "work", "yard", "year", "your", "zero", "zone",
        # 5+ char common words that are tickers
        "about", "above", "actor", "after", "again", "agent", "agree", "ahead", "alarm", "alert", "alice", "align", "alive", "allow", "alone", "along", "alpha", "alter", "angel", "anger", "angle", "angry", "apart", "apple", "apply", "argue", "arise", "armed", "armor", "array", "arrow", "asset", "atlas", "avoid", "awake", "award", "aware", "badly", "basic", "beach", "began", "begin", "being", "below", "bench", "birth", "black", "blame", "blank", "block", "blood", "board", "boost", "bound", "brain", "brand", "brave", "bread", "break", "breed", "brief", "bring", "broad", "broke", "brown", "build", "burst", "buyer", "cabin", "cable", "catch", "cause", "chain", "chair", "chaos", "charm", "chart", "chase", "cheap", "check", "chest", "chief", "child", "china", "chose", "claim", "class", "clean", "clear", "click", "climb", "clock", "close", "cloud", "coach", "coast", "coats", "color", "comes", "comic", "coral", "costs", "could", "count", "court", "cover", "craft", "crash", "crazy", "cream", "crime", "crops", "cross", "crowd", "crown", "crude", "curve", "cycle", "daily", "dance", "dated", "dealt", "death", "debut", "delay", "depth", "devil", "diary", "dirty", "doing", "doubt", "dozen", "draft", "drama", "drank", "dream", "dress", "dried", "drill", "drink", "drive", "drove", "drugs", "dry", "ducks", "early", "earth", "eight", "elite", "empty", "enemy", "enjoy", "enter", "entry", "equal", "error", "event", "every", "exact", "exist", "extra", "faith", "false", "fault", "fence", "fiber", "field", "fight", "final", "finds", "first", "fixed", "flags", "flame", "flash", "fleet", "flesh", "flies", "floor", "flour", "flows", "focus", "folks", "force", "forth", "found", "frame", "frank", "fraud", "fresh", "front", "fruit", "funds", "funny", "gains", "games", "gates", "giant", "gifts", "given", "glass", "globe", "glory", "glove", "goals", "going", "grace", "grade", "grain", "grand", "grant", "graph", "grass", "grave", "great", "green", "greet", "gross", "group", "grown", "grows", "guard", "guess", "guest", "guide", "happy", "harsh", "heart", "heavy", "helps", "hence", "herbs", "hides", "hills", "hints", "hired", "holds", "holes", "homes", "honor", "hooks", "horse", "hotel", "hours", "house", "human", "humor", "hurts", "ideal", "ideas", "image", "index", "inner", "input", "issue", "items", "japan", "japan", "jewel", "joins", "jokes", "judge", "juice", "keeps", "kills", "kinds", "kings", "knife", "knock", "known", "knows", "label", "labor", "lacks", "lakes", "large", "laser", "later", "laugh", "layer", "leads", "learn", "lease", "least", "leave", "legal", "level", "lewis", "light", "limit", "lines", "links", "lions", "lives", "loans", "local", "locks", "logic", "looks", "loops", "loose", "lords", "loses", "loved", "loves", "lower", "lucky", "lunch", "lying", "magic", "major", "makes", "march", "marks", "match", "maybe", "mayor", "meals", "means", "meant", "media", "meets", "metal", "meter", "might", "miles", "minds", "minor", "mixed", "modal", "model", "money", "month", "moral", "motor", "mount", "mouse", "mouth", "moved", "moves", "movie", "music", "needs", "nerve", "never", "night", "nodes", "noise", "north", "notes", "nurse", "ocean", "offer", "often", "older", "opens", "opera", "order", "other", "ought", "outer", "owned", "owner", "pages", "paint", "pairs", "panel", "paper", "parks", "parts", "party", "patch", "paths", "peace", "phase", "phone", "photo", "piano", "piece", "pilot", "pipes", "pitch", "pizza", "place", "plain", "plane", "plans", "plant", "plate", "plays", "plaza", "plots", "poems", "point", "pools", "pound", "power", "press", "price", "pride", "prime", "print", "prior", "prize", "proof", "proud", "prove", "pulls", "pumps", "punch", "pupil", "purse", "quest", "quick", "quiet", "quite", "quote", "radio", "raise", "range", "rapid", "rates", "reach", "reads", "ready", "realm", "rebel", "refer", "relax", "reply", "right", "rings", "rises", "risks", "river", "roads", "robot", "rocks", "roles", "rolls", "roots", "rough", "round", "route", "royal", "rules", "rural", "safer", "sales", "scale", "scale", "scene", "scope", "score", "scout", "scrub", "seats", "seems", "sells", "sense", "serve", "setup", "seven", "shade", "shake", "shall", "shame", "shape", "share", "sharp", "sheep", "sheet", "shelf", "shell", "shift", "shine", "shirt", "shock", "shoes", "shoot", "short", "shown", "shows", "sides", "sight", "signs", "silly", "since", "sites", "sixth", "sized", "sizes", "skill", "skull", "sleep", "slide", "slope", "slots", "small", "smart", "smile", "smoke", "snake", "snow", "socks", "solar", "solid", "solve", "songs", "sorry", "sorts", "souls", "sound", "south", "space", "spare", "speak", "speed", "spell", "spend", "spent", "split", "spoke", "sport", "spots", "spray", "staff", "stage", "stake", "stamp", "stand", "stars", "start", "state", "stays", "steal", "steam", "steel", "steps", "stick", "still", "stock", "stone", "stood", "stops", "store", "storm", "story", "strip", "stuck", "study", "stuff", "style", "sugar", "suits", "sunny", "super", "sweet", "swing", "table", "takes", "talks", "tanks", "tasks", "taste", "taxes", "teach", "teams", "tears", "tells", "terms", "tests", "thank", "theft", "their", "theme", "there", "these", "thick", "thing", "think", "third", "those", "three", "threw", "throw", "thumb", "tides", "tiger", "tight", "times", "tired", "title", "today", "token", "tools", "tooth", "topic", "total", "touch", "tough", "tower", "towns", "track", "trade", "trail", "train", "treat", "trees", "trend", "trial", "tribe", "trick", "tried", "tries", "trips", "truck", "truly", "trunk", "trust", "truth", "tubes", "turns", "twice", "twist", "types", "ultra", "uncle", "under", "union", "unity", "until", "upper", "upset", "urban", "urged", "usage", "users", "using", "usual", "valid", "value", "video", "views", "virus", "visit", "vital", "voice", "votes", "wages", "waits", "walks", "walls", "wants", "warns", "waste", "watch", "water", "waves", "wealthy", "weeks", "weigh", "weird", "wells", "wheel", "where", "which", "while", "white", "whole", "whose", "wider", "wilde", "winds", "wines", "wings", "wins", "wipes", "wired", "wires", "witch", "woman", "women", "woods", "words", "works", "world", "worry", "worse", "worst", "worth", "would", "write", "wrong", "wrote", "years", "young", "youth", "bullish",
    ]
)

def make_reddit_client(s: Settings) -> praw.Reddit:
    """Create a PRAW Reddit client using installed-app refresh token flow.
    
    Installed apps use empty client_secret and authenticate via refresh token.
    """
    if not s.REDDIT_CLIENT_ID:
        raise RuntimeError("Missing REDDIT_CLIENT_ID")
    # Normalize secret: installed apps require empty string
    client_secret = ""  # force installed-app flow
    if not s.REDDIT_REFRESH_TOKEN:
        raise RuntimeError("Missing REDDIT_REFRESH_TOKEN")
    
    try:
        reddit = praw.Reddit(
            client_id=s.REDDIT_CLIENT_ID,
            client_secret=client_secret,
            user_agent=s.REDDIT_USER_AGENT,
            refresh_token=s.REDDIT_REFRESH_TOKEN,
        )
        reddit.user.me()  # Preflight auth check
        info("reddit_auth_successful")
        return reddit
    except prawcore.exceptions.PrawcoreException as e:
        err("reddit_auth_failed", error=str(e))
        raise  # Re-raise after logging
    except Exception as e:
        err("reddit_client_init_failed", error=str(e))
        raise


def fetch_alias_map(conn: Connection) -> Tuple[dict, dict, dict, dict, Set[str]]:
    """Load and index all assets/aliases for universe='stock' from database.
    
    Returns:
        - alias_to_asset: lower(alias) -> (asset_id, SYMBOL)
        - ticker_to_asset: lower(ticker) -> (asset_id, SYMBOL) 
        - asset_id_to_ticker: asset_id -> SYMBOL (canonical ticker)
        - multi_alias_index: first_token -> list[(alias_tokens[], asset_id, SYMBOL)] (longest-first)
        - aliases_equal_ticker: set of aliases that equal their ticker (e.g., "t" for ticker T)
    """
    # Fetch all stock assets (tickers and names)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select a.id as asset_id, a.ticker, lower(a.ticker) as ticker_lc, lower(a.name) as name_lc
            from assets a
            where a.universe = 'stock'
            """
        )
        assets_rows = cur.fetchall()

        # Fetch all aliases for stock assets
        cur.execute(
            """
            select aa.asset_id, lower(aa.alias) as alias_lc
            from asset_aliases aa
            inner join assets a on a.id = aa.asset_id
            where a.universe = 'stock'
            """
        )
        alias_rows = cur.fetchall()

    # Initialize lookup dictionaries
    alias_to_asset: Dict[str, Tuple[int, str]] = {}
    ticker_to_asset: Dict[str, Tuple[int, str]] = {}
    asset_id_to_ticker: Dict[int, str] = {}
    # For efficient multi-word alias search: first_token -> list of (alias_tokens, asset_id, SYMBOL)
    multi_alias_index: Dict[str, List[Tuple[List[str], int, str]]] = {}
    aliases_equal_ticker: Set[str] = set()  # Tracks aliases identical to their ticker

    # Build ticker and asset name mappings
    for r in assets_rows:
        ticker_to_asset[r["ticker_lc"]] = (r["asset_id"], r["ticker"])
        asset_id_to_ticker[int(r["asset_id"])] = r["ticker"]
        if r.get("name_lc"):
            alias_to_asset[r["name_lc"]] = (r["asset_id"], r["ticker"])
    
    # Process all aliases
    for r in alias_rows:
        alias = r["alias_lc"].strip()
        if not alias:
            continue
        asset_id = int(r["asset_id"])
        sym = asset_id_to_ticker.get(asset_id)
        
        # Split alias into tokens by whitespace for single vs multi-word handling
        alias_tokens = alias.split()
        if len(alias_tokens) == 1:
            # Single-word alias
            alias_to_asset[alias_tokens[0]] = (asset_id, sym)
            # Track if this alias is identical to the ticker (case-insensitive)
            if sym and sym.lower() == alias_tokens[0]:
                aliases_equal_ticker.add(alias_tokens[0])
        else:
            # Multi-word alias: index by first token for efficient lookup
            first = alias_tokens[0]
            multi_alias_index.setdefault(first, []).append((alias_tokens, asset_id, sym or ""))

    # Sort multi-alias lists by descending length (greedy longest match)
    for first, lst in multi_alias_index.items():
        lst.sort(key=lambda t: len(t[0]), reverse=True)

    return alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index, aliases_equal_ticker


def iter_mentions(text: str,
                  alias_to_asset: Dict[str, Tuple[int, str | None]],
                  ticker_to_asset: Dict[str, Tuple[int, str | None]],
                  multi_alias_index: Dict[str, List[Tuple[List[str], int, str]]],
                  aliases_equal_ticker: Set[str]) -> Iterable[Tuple[int, str, str]]:
    """Fast-path mention detection within one comment.
    
    Detection rules:
    - Tickers: Accept if $prefixed or ALL-CAPS; ambiguous words require explicit form
    - Single-word alias==ticker: Enforce ticker rule; other aliases: accept as-is
    - Multi-word aliases: Greedy matching with boundary checks
    
    Returns tuples of (asset_id, SYMBOL, mention_surface_text).
    """
    # Keep original and lowercased versions for case analysis
    lower = text.lower()
    
    # Use word boundary regex to ensure we only match complete words
    # This pattern captures $-prefixed tokens and regular alphanumeric tokens
    # with proper word boundaries (whitespace, punctuation, start/end of string)
    word_pattern = r'(?:^|(?<=\s)|(?<=\W))(\$?[a-z0-9.-]+)(?=\s|(?=\W)|$)'
    lower_matches = re.findall(word_pattern, lower)
    
    # Same pattern for original case to detect $ and ALL-CAPS
    orig_pattern = r'(?:^|(?<=\s)|(?<=\W))(\$?[A-Za-z0-9.-]+)(?=\s|(?=\W)|$)'
    orig_matches = re.findall(orig_pattern, text)

    # Map lower token -> observed originals (to detect ALL CAPS or $ prefix)
    from collections import defaultdict
    originals_by_lower: DefaultDict[str, Set[str]] = defaultdict(set)
    for ot in orig_matches:
        originals_by_lower[ot.lower()].add(ot)

    token_set: Set[str] = set(lower_matches)
    seen: Set[Tuple[int, str, str]] = set()  # Dedupe results

    # Single-token detection (tickers and single-word aliases)
    for tok in token_set:
        tok_stripped = tok[1:] if tok.startswith("$") else tok  # Remove $ prefix for lookup
        originals = originals_by_lower.get(tok, set()) | originals_by_lower.get(tok_stripped, set())
        
        def is_explicit_symbol(o: str) -> bool:
            """Check if token is explicitly formatted as stock symbol ($TICKER or ALL-CAPS)."""
            return o.startswith("$") or (o.isupper() and any(c.isalpha() for c in o))

        # TICKER DETECTION: Accept all tickers unless they're ambiguous words
        if tok_stripped in ticker_to_asset:
            # If it's an ambiguous word (like "so", "it"), require explicit formatting ($TICKER or ALL-CAPS)
            if tok_stripped in AMBIGUOUS_TICKER_WORDS:
                if any(is_explicit_symbol(ot) for ot in originals):
                    pair = ticker_to_asset[tok_stripped]
                    mention_text = next(iter(originals)) if originals else tok
                    seen.add((pair[0], pair[1] or tok_stripped.upper(), mention_text))
            else:
                # Not ambiguous, accept any case (AAPL, aapl, Aapl all work)
                pair = ticker_to_asset[tok_stripped]
                mention_text = next(iter(originals)) if originals else tok
                seen.add((pair[0], pair[1] or tok_stripped.upper(), mention_text))

        # ALIAS DETECTION: Single-word aliases
        if tok in alias_to_asset:
            asset_id, sym = alias_to_asset[tok]
            mention_text = next(iter(originals_by_lower.get(tok, {tok})))
            # If alias equals ticker (e.g., "t" for ticker T), apply same rules as tickers
            if tok in aliases_equal_ticker:
                if tok in AMBIGUOUS_TICKER_WORDS:
                    if any(is_explicit_symbol(ot) for ot in originals):
                        seen.add((asset_id, sym or tok.upper(), mention_text))
                else:
                    # Not ambiguous, accept any case
                    seen.add((asset_id, sym or tok.upper(), mention_text))
            else:
                # Regular alias, accept as-is
                seen.add((asset_id, sym or tok, mention_text))
        
        # Check stripped version for aliases too (handles $prefixed aliases)
        if tok_stripped in alias_to_asset:
            asset_id, sym = alias_to_asset[tok_stripped]
            mention_text = next(iter(originals_by_lower.get(tok_stripped, {tok_stripped})))
            if tok_stripped in aliases_equal_ticker:
                if tok_stripped in AMBIGUOUS_TICKER_WORDS:
                    if any(is_explicit_symbol(ot) for ot in originals):
                        seen.add((asset_id, sym or tok_stripped.upper(), mention_text))
                else:
                    # Not ambiguous, accept any case
                    seen.add((asset_id, sym or tok_stripped.upper(), mention_text))
            else:
                seen.add((asset_id, sym or tok_stripped, mention_text))

    # MULTI-WORD ALIAS DETECTION: Greedy matching with proper word boundaries
    for tok in token_set:
        if tok in multi_alias_index:
            for alias_tokens, asset_id, sym in multi_alias_index[tok]:
                alias_lower = " ".join(alias_tokens)
                # Use word boundary regex to ensure the full phrase is bounded by word boundaries
                alias_pattern = r'(?:^|(?<=\s)|(?<=\W))' + re.escape(alias_lower) + r'(?=\s|(?=\W)|$)'
                if re.search(alias_pattern, lower):
                    # If alias equals ticker (single-token case), enforce explicit symbol (rare edge case)
                    if len(alias_tokens) == 1 and alias_tokens[0] in aliases_equal_ticker:
                        if any(o.startswith("$") or (o.isupper() and any(c.isalpha() for c in o)) for o in originals_by_lower.get(alias_tokens[0], {alias_tokens[0]})):
                            seen.add((asset_id, sym or alias_tokens[0].upper(), alias_lower))
                    else:
                        # Multi-word alias found (e.g., "service now" -> NOW)
                        seen.add((asset_id, sym or alias_lower, alias_lower))

    # Return all detected mentions
    for item in seen:
        yield item


def clean_text(text: str, max_len: int = 800) -> str:
    """Normalize text for LLM: remove non-alnum/$, compress spaces, trim length."""
    text = re.sub(r"[^a-zA-Z0-9\s$]", " ", text)  # Keep alphanumeric, spaces, and $ only
    text = re.sub(r"\s+", " ", text).strip()      # Collapse multiple spaces
    if len(text) > max_len:
        return text[:max_len]                     # Truncate if too long
    return text


def score_sentiment_llm(client: OpenAI, model: str, body: str, symbol: str | None, mention: str | None) -> tuple[int, str]:
    """Call LLM to classify sentiment for a stock mention.

    Returns (sentiment_int, label_str). Always returns a sentiment classification.
    """
    # Clean the comment text for the LLM
    cleaned = clean_text(body)
    symbol_str = symbol or ""
    mention_str = (mention or symbol_str or "").strip()

    try:
        # Call OpenAI API with minimal sentiment classification prompt
        resp = client.chat.completions.create(
            model=model,
            temperature=0,        # Deterministic responses
            max_tokens=4,         # Very short response expected
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Classify sentiment for stock mention in comment.\n"
                        "Consider only the specified SYMBOL.\n"
                        "Return exactly one word:\n"
                        "Bullish (positive, buy, long, calls, up)\n"
                        "Bearish (negative, sell, short, puts, down)\n"
                        "Neutral (news, questions, mixed)"
                    ),
                },
                {"role": "user", "content": f"SYMBOL: {symbol_str}\nMENTION: {mention_str}\nCOMMENT: {cleaned}"},
            ],
        )

        # Parse the response
        answer = (resp.choices[0].message.content or "").strip().lower()
        if "bull" in answer:
            return 1, "Bullish"           # Positive sentiment
        if "bear" in answer:
            return -1, "Bearish"          # Negative sentiment
        return 0, "Neutral"               # Default/neutral sentiment

    except APIError as e:
        err("openai_api_error", error=str(e), status_code=e.status_code, error_type=e.type)
        return 0, "Neutral"               # Fallback on API error
    except Exception as e:
        err("openai_unhandled_error", error=str(e))
        return 0, "Neutral"               # Fallback on other errors


def load_sources(conn: Connection) -> dict:
    """Fetch all sources into a lowercased name → id mapping (e.g., '/r/wallstreetbets' → 1)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select id, name from sources")
        rows = cur.fetchall()
    # Map lowercased name -> id for subreddit matching
    return {str(r["name"]).lower(): int(r["id"]) for r in rows}


@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
def insert_comment(conn: Connection, asset_id: int, source_id: int, commented_at: datetime, sentiment: int, body: str, link: str) -> None:
    """Insert one comment row for (asset, source) at a given time. Retries on failure."""
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into comments(asset_id, source_id, commented_at, sentiment, body, link)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (asset_id, source_id, commented_at, sentiment, body, link),
        )


def process_comment(
    comment: praw.models.Comment,
    conn: Connection,
    openai_client: OpenAI,
    model: str,
    alias_to_asset: Dict,
    ticker_to_asset: Dict,
    multi_alias_index: Dict,
    aliases_equal_ticker: Set,
    source_name_to_id: Dict,
    asset_id_to_ticker: Dict,
):
    """Process a single comment: detect mentions, classify, and insert."""
    body = comment.body or ""
    
    # STEP 1: Fast-path mention detection
    mentions = list(set(iter_mentions(body, alias_to_asset, ticker_to_asset, multi_alias_index, aliases_equal_ticker)))
    if not mentions:
        return  # No potential mentions found
    
    # STEP 2: LLM classification for each mention
    kept: List[Tuple[int, str, str]] = []       # All detected mentions
    sentiments: Dict[int, int] = {}             # asset_id -> sentiment score
    sentiment_labels: Dict[int, str] = {}       # asset_id -> sentiment label

    for asset_id, sym, mention_text in mentions:
        sent, sent_label = score_sentiment_llm(openai_client, model, body, sym, mention_text)
        kept.append((asset_id, sym, mention_text))
        sentiments[asset_id] = sent
        sentiment_labels[asset_id] = sent_label
        
    if not kept:
        return  # No mentions to process
    
    # STEP 3: Prepare database insert data
    created = datetime.fromtimestamp(float(comment.created_utc), tz=timezone.utc)
    link = f"https://reddit.com{getattr(comment, 'permalink', '')}"
    src_name = f"/r/{getattr(getattr(comment, 'subreddit', None), 'display_name', '').strip()}".lower()
    source_id = source_name_to_id.get(src_name)
    if not source_id:
        err("unknown_source", src=src_name)
        return
    
    # STEP 4: Insert to database (dedupe to one row per asset per comment)
    inserted_asset_ids: List[int] = []
    for asset_id in {aid for (aid, _, _) in kept}:  # Unique asset IDs only
        sent = sentiments[asset_id]
        insert_comment(conn, asset_id, source_id, created, sent, body, link)
        inserted_asset_ids.append(asset_id)
    
    # STEP 5: Log successful inserts with sentiment decisions
    tickers = sorted({ asset_id_to_ticker.get(aid, "") for aid in inserted_asset_ids if asset_id_to_ticker.get(aid) })
    preview = body[:220] + ("..." if len(body) > 220 else "")
    # Include LLM sentiment decisions in the log
    decisions = {asset_id_to_ticker.get(aid, ""): sentiment_labels.get(aid, "") for aid in inserted_asset_ids if asset_id_to_ticker.get(aid)}
    info("mention_inserted", n=len(inserted_asset_ids), tickers=tickers, decisions=decisions, src=src_name, link=link, preview=preview)


def make_db_connection(db_url: str) -> Connection:
    """Connect to the database, with autocommit enabled."""
    conn = connect(db_url)
    conn.autocommit = True
    return conn


def ensure_db_connection(conn: Connection | None, db_url: str) -> Connection:
    """Ensure the database connection is alive, reconnecting if necessary."""
    if conn is None or conn.closed:
        info("db_reconnecting")
        return make_db_connection(db_url)
    try:
        # A lightweight query to check if the connection is still valid
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception as e:
        err("db_connection_lost", error=str(e))
        info("db_reconnecting")
        return make_db_connection(db_url)
    return conn


def main():
    """Entry point: stream comments, detect mentions, classify, and insert."""
    # Load configuration
    s = Settings()
    subreddits = load_subreddits(s.SUBREDDITS_CONFIG_PATH, s.SUBREDDITS)
    if not subreddits:
        raise RuntimeError(f"No subreddits configured for {s.SERVICE_NAME or 'reddit scraper'}")

    # Initialize OpenAI client
    if not s.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required")
    openai_client = OpenAI(api_key=s.OPENAI_API_KEY)
    model = s.OPENAI_MODEL

    # Initialize Reddit client and test authentication
    reddit = make_reddit_client(s)

    # Connect to database
    if not s.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    conn = make_db_connection(s.DATABASE_URL)

    # Build detection indices and source mappings
    alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index, aliases_equal_ticker = fetch_alias_map(conn)
    source_name_to_id = load_sources(conn)
    last_alias_refresh = time.time()
    ALIAS_REFRESH_INTERVAL = 3600  # 1 hour

    # Start streaming comments from all configured subreddits
    # Reddit API accepts multiple subreddits joined with '+'
    multi = "+".join(subreddits)

    # Main processing loop
    while True:
        try:
            # Periodically refresh asset aliases from DB
            if time.time() - last_alias_refresh > ALIAS_REFRESH_INTERVAL:
                info("alias_map_reloading")
                conn = ensure_db_connection(conn, s.DATABASE_URL)
                alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index, aliases_equal_ticker = fetch_alias_map(conn)
                last_alias_refresh = time.time()
                info("alias_map_reloaded")

            info("stream_start", subreddits=subreddits)
            stream = reddit.subreddit(multi).stream.comments(skip_existing=True)
            for comment in stream:
                try:
                    # Ensure database is connected before processing
                    conn = ensure_db_connection(conn, s.DATABASE_URL)
                    process_comment(
                        comment,
                        conn,
                        openai_client,
                        model,
                        alias_to_asset,
                        ticker_to_asset,
                        multi_alias_index,
                        aliases_equal_ticker,
                        source_name_to_id,
                        asset_id_to_ticker,
                    )
                except Exception as e:
                    # Error processing a single comment, log it and continue
                    comment_id = getattr(comment, "id", None)
                    err("process_comment_error", error=str(e), comment_id=comment_id)
                    time.sleep(1)  # Brief pause on error before continuing

        except prawcore.exceptions.PrawcoreException as e:
            err("stream_error_prawcore", error=str(e))
            info("stream_restarting_delay", delay=15)
            time.sleep(15)  # Wait 15s before restarting stream on PRAW error
        except Exception as e:
            err("stream_loop_unhandled_error", error=str(e))
            info("stream_restarting_delay", delay=60)
            time.sleep(60)  # Longer wait for unknown errors before restarting


if __name__ == "__main__":
    main()

