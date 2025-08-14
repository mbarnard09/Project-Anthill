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

from openai import OpenAI

from core.config import Settings
from core.logging import info, err


def _mask(v: str | None, keep: int = 6) -> str | None:
    if not v:
        return None
    if len(v) <= keep:
        return v
    return v[:keep] + "…"


def load_subreddits(cfg_path: str, env_fallback: str | None) -> List[str]:
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        subs = data.get("subreddits", [])
        return [s.strip() for s in subs if s and isinstance(s, str)]
    if env_fallback:
        return [s.strip() for s in env_fallback.split(",") if s.strip()]
    return []


def make_reddit_client(s: Settings) -> praw.Reddit:
    if not s.REDDIT_CLIENT_ID:
        raise RuntimeError("Missing REDDIT_CLIENT_ID")
    # Normalize secret: installed apps require empty string
    client_secret = ""  # force installed-app flow
    if not s.REDDIT_REFRESH_TOKEN:
        raise RuntimeError("Missing REDDIT_REFRESH_TOKEN")
    reddit = praw.Reddit(
        client_id=s.REDDIT_CLIENT_ID,
        client_secret=client_secret,
        user_agent=s.REDDIT_USER_AGENT,
        refresh_token=s.REDDIT_REFRESH_TOKEN,
    )
    return reddit


def fetch_alias_map(conn: Connection) -> Tuple[dict, dict, dict, dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select a.id as asset_id, a.ticker, lower(a.ticker) as ticker_lc, lower(a.name) as name_lc
            from assets a
            where a.universe = 'stock'
            """
        )
        assets_rows = cur.fetchall()

        cur.execute(
            """
            select aa.asset_id, lower(aa.alias) as alias_lc
            from asset_aliases aa
            inner join assets a on a.id = aa.asset_id
            where a.universe = 'stock'
            """
        )
        alias_rows = cur.fetchall()

    alias_to_asset: Dict[str, Tuple[int, str]] = {}
    ticker_to_asset: Dict[str, Tuple[int, str]] = {}
    asset_id_to_ticker: Dict[int, str] = {}
    # For efficient multi-word alias search
    multi_alias_index: Dict[str, List[Tuple[str, int, str | None]]] = {}

    for r in assets_rows:
        ticker_to_asset[r["ticker_lc"]] = (r["asset_id"], r["ticker"])
        asset_id_to_ticker[int(r["asset_id"])] = r["ticker"]
        if r.get("name_lc"):
            alias_to_asset[r["name_lc"]] = (r["asset_id"], r["ticker"])
    for r in alias_rows:
        alias = r["alias_lc"]
        # include the ticker so alias logic can compare alias==ticker
        tick = asset_id_to_ticker.get(int(r["asset_id"]))
        alias_to_asset[alias] = (r["asset_id"], tick)
    # Build multi/single alias index
    for alias, pair in list(alias_to_asset.items()):
        if not alias:
            continue
        if " " in alias:
            first = alias.split()[0]
            multi_alias_index.setdefault(first, []).append((alias, pair[0], pair[1]))

    return alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index


def iter_mentions(text: str,
                  alias_to_asset: Dict[str, Tuple[int, str | None]],
                  ticker_to_asset: Dict[str, Tuple[int, str | None]],
                  multi_alias_index: Dict[str, List[Tuple[str, int, str | None]]]) -> Iterable[Tuple[int, str, str]]:
    # Keep original and lowercased versions
    lower = text.lower()
    padded = f" {lower} "

    # Tokenize; keep $ and alnum
    lower_tokens: List[str] = re.findall(r"\$?[a-z0-9]+", lower)
    orig_tokens: List[str] = re.findall(r"\$?[A-Za-z0-9]+", text)

    token_set: Set[str] = set(lower_tokens)

    # Map lower token -> observed originals (to detect ALL CAPS or $ prefix)
    from collections import defaultdict
    originals_by_lower: DefaultDict[str, Set[str]] = defaultdict(set)
    for ot in orig_tokens:
        originals_by_lower[ot.lower()].add(ot)

    seen: Set[Tuple[int, str, str]] = set()

    # Single-token tickers and aliases (explicit mention rules)
    for tok in token_set:
        tok_stripped = tok[1:] if tok.startswith("$") else tok
        originals = originals_by_lower.get(tok, set()) | originals_by_lower.get(tok_stripped, set())
        # Ticker rule: count only if $TICKER or ALL CAPS symbol
        if tok_stripped in ticker_to_asset:
            explicit = False
            for ot in originals:
                if ot.startswith("$") or (ot.isupper() and ot.isalpha()):
                    explicit = True
                    break
            if explicit:
                pair = ticker_to_asset[tok_stripped]
                # use original form of token for prompt context
                mention_text = next(iter(originals)) if originals else tok
                seen.add((pair[0], pair[1] or tok_stripped.upper(), mention_text))

        # Alias rule:
        # - If alias equals the symbol, enforce same explicit rule ($ or ALL CAPS)
        # - Else accept alias as-is
        if tok in alias_to_asset:
            asset_id, maybe_ticker = alias_to_asset[tok]
            mention_text = next(iter(originals_by_lower.get(tok, {tok})))
            if maybe_ticker and maybe_ticker.lower() == tok:
                if any(ot.startswith("$") or (ot.isupper() and ot.isalpha()) for ot in originals):
                    seen.add((asset_id, maybe_ticker, mention_text))
            else:
                seen.add((asset_id, maybe_ticker or tok, mention_text))
        if tok_stripped in alias_to_asset:
            asset_id, maybe_ticker = alias_to_asset[tok_stripped]
            mention_text = next(iter(originals_by_lower.get(tok_stripped, {tok_stripped})))
            if maybe_ticker and maybe_ticker.lower() == tok_stripped:
                if any(ot.startswith("$") or (ot.isupper() and ot.isalpha()) for ot in originals):
                    seen.add((asset_id, maybe_ticker, mention_text))
            else:
                seen.add((asset_id, maybe_ticker or tok_stripped, mention_text))

    # Multi-word aliases: indexed by first token, check with whitespace boundaries
    first_tokens = token_set
    for tok in first_tokens:
        if tok in multi_alias_index:
            for alias, asset_id, ticker in multi_alias_index[tok]:
                if f" {alias} " in padded:
                    seen.add((asset_id, ticker or alias, alias))

    for item in seen:
        yield item


def clean_text(text: str, max_len: int = 800) -> str:
    text = re.sub(r"[^a-zA-Z0-9\s$]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        return text[:max_len]
    return text


def score_sentiment_llm(client: OpenAI, model: str, body: str, symbol: str | None, mention: str | None) -> tuple[int | None, str]:
    cleaned = clean_text(body)
    symbol_str = symbol or ""
    mention_str = (mention or symbol_str or "").strip()
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            max_tokens=6,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You classify sentiment for a specific stock mention in a forum comment. "
                        "Given the canonical stock symbol (SYMBOL) and the exact mention text (MENTION) found in the comment, decide if the comment is referring to that stock. "
                        "If not referring to the stock (e.g., the word is a normal English word like 'on', 'it', 'are', 'so', 'well', etc., or the context is not about markets), respond Not Stock. "
                        "If it is referring to the stock, classify sentiment as Bullish, Bearish, or Neutral. "
                        "Return exactly one of these words: Bullish, Bearish, Neutral, Not Stock."
                    ),
                },
                {"role": "user", "content": f"SYMBOL: {symbol_str}\nMENTION: {mention_str}\nCOMMENT: {cleaned}"},
            ],
        )
        answer = (resp.choices[0].message.content or "").strip().lower()
        if "not stock" in answer:
            return None, "Not Stock"
        if "bull" in answer:
            return 1, "Bullish"
        if "bear" in answer:
            return -1, "Bearish"
        return 0, "Neutral"
    except Exception as e:
        err("openai_error", error=str(e))
        return 0, "Neutral"


def load_sources(conn: Connection) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select id, name from sources")
        rows = cur.fetchall()
    # map lowercased name -> id (e.g., '/r/wallstreetbets')
    return {str(r["name"]).lower(): int(r["id"]) for r in rows}


@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
def insert_comment(conn: Connection, asset_id: int, source_id: int, commented_at: datetime, sentiment: int, body: str, link: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into comments(asset_id, source_id, commented_at, sentiment, body, link)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (asset_id, source_id, commented_at, sentiment, body, link),
        )


def main():
    s = Settings()
    subreddits = load_subreddits(s.SUBREDDITS_CONFIG_PATH, s.SUBREDDITS)
    if not subreddits:
        raise RuntimeError("No subreddits configured for reddit1")

    # Quiet startup (only log on mentions)

    if not s.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required")
    openai_client = OpenAI(api_key=s.OPENAI_API_KEY)
    model = s.OPENAI_MODEL

    reddit = make_reddit_client(s)
    # Preflight auth check; fallback to access token if refresh fails
    try:
        reddit.user.me()
    except prawcore.exceptions.ResponseException as e:
        # Silent unless failure blocks streaming
        err("reddit_auth_failed", status=getattr(e, 'response', None).status_code if hasattr(e, 'response') else None)
        raise

    if not s.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    conn = connect(s.DATABASE_URL)
    conn.autocommit = True

    alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index = fetch_alias_map(conn)
    source_name_to_id = load_sources(conn)

    # Stream new comments from subreddits (use '+' join for multi-subreddit in Reddit API)
    multi = "+".join(subreddits)
    streams = [reddit.subreddit(multi).stream.comments(skip_existing=True)]

    info("stream_start", subreddits=subreddits)
    for comment in streams[0]:
        try:
            body = comment.body or ""
            mentions = list(set(iter_mentions(body, alias_to_asset, ticker_to_asset, multi_alias_index)))
            if not mentions:
                continue
            # Classify each mention independently; keep only those not flagged as Not Stock
            kept: List[Tuple[int, str, str]] = []
            sentiments: Dict[int, int] = {}
            sentiment_labels: Dict[int, str] = {}
            for asset_id, sym, mention_text in mentions:
                sent, sent_label = score_sentiment_llm(openai_client, model, body, sym, mention_text)
                if sent is None:
                    continue
                kept.append((asset_id, sym, mention_text))
                sentiments[asset_id] = sent
                sentiment_labels[asset_id] = sent_label
            if not kept:
                continue
            created = datetime.fromtimestamp(float(comment.created_utc), tz=timezone.utc)
            link = f"https://reddit.com{getattr(comment, 'permalink', '')}"
            src_name = f"/r/{getattr(getattr(comment, 'subreddit', None), 'display_name', '').strip()}".lower()
            source_id = source_name_to_id.get(src_name)
            if not source_id:
                err("unknown_source", src=src_name)
                continue
            inserted_asset_ids: List[int] = []
            # Insert per kept mention (per asset). If duplicates for same asset, insert once per comment.
            for asset_id in {aid for (aid, _, _) in kept}:
                sent = sentiments[asset_id]
                insert_comment(conn, asset_id, source_id, created, sent, body, link)
                inserted_asset_ids.append(asset_id)
            # Log only what we actually inserted
            tickers = sorted({ asset_id_to_ticker.get(aid, "") for aid in inserted_asset_ids if asset_id_to_ticker.get(aid) })
            preview = body[:220]
            # If multiple assets, prefer a neutral summary; per-asset sentiments are in DB
            info("mention_inserted", n=len(inserted_asset_ids), tickers=tickers, src=src_name, link=link, preview=preview)
        except Exception as e:
            err("process_error", error=str(e))
            time.sleep(1)


if __name__ == "__main__":
    main()

