import re
import time
import html
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Set, Tuple

from psycopg import Connection
from psycopg import connect
from psycopg.rows import dict_row

from openai import OpenAI, APIError

from core.config import Settings
from core.logging import info, err
from core.ambiguity import BLACKLISTED_TICKERS


def clean_html_to_text(s: str) -> str:
	# Replace <br> with space, strip tags, unescape entities
	s = s.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
	s = re.sub(r"<[^>]+>", " ", s)
	s = html.unescape(s)
	# Normalize whitespace
	s = re.sub(r"\s+", " ", s).strip()
	return s


def clean_text(text: str, max_len: int = 800) -> str:
	text = re.sub(r"[^a-zA-Z0-9\s$]", " ", text)
	text = re.sub(r"\s+", " ", text).strip()
	if len(text) > max_len:
		return text[:max_len]
	return text


def score_sentiment_llm(client: OpenAI, model: str, body: str, symbol: str | None, mention: str | None) -> tuple[int, str]:
	cleaned = clean_text(body)
	symbol_str = symbol or ""
	mention_str = (mention or symbol_str or "").strip()
	try:
		resp = client.chat.completions.create(
			model=model,
			temperature=0,
			max_tokens=4,
			messages=[
				{
					"role": "system",
					"content": (
						"Classify sentiment for crypto asset mention in comment.\n"
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
		answer = (resp.choices[0].message.content or "").strip().lower()
		if "bull" in answer:
			return 1, "Bullish"
		if "bear" in answer:
			return -1, "Bearish"
		return 0, "Neutral"
	except APIError as e:
		err("openai_api_error", error=str(e), status_code=e.status_code, error_type=e.type)
		return 0, "Neutral"
	except Exception as e:
		err("openai_unhandled_error", error=str(e))
		return 0, "Neutral"


def fetch_alias_map(conn: Connection) -> Tuple[dict, dict, dict, dict, Set[str]]:
	with conn.cursor(row_factory=dict_row) as cur:
		cur.execute(
			"""
			select a.id as asset_id, a.ticker, lower(a.ticker) as ticker_lc, lower(a.name) as name_lc
			from assets a
			where a.universe = 'crypto'
			"""
		)
		assets_rows = cur.fetchall()
		cur.execute(
			"""
			select aa.asset_id, lower(aa.alias) as alias_lc
			from asset_aliases aa
			inner join assets a on a.id = aa.asset_id
			where a.universe = 'crypto'
			"""
		)
		alias_rows = cur.fetchall()

	alias_to_asset: Dict[str, Tuple[int, str]] = {}
	ticker_to_asset: Dict[str, Tuple[int, str]] = {}
	asset_id_to_ticker: Dict[int, str] = {}
	multi_alias_index: Dict[str, List[Tuple[List[str], int, str]]] = {}
	aliases_equal_ticker: Set[str] = set()

	for r in assets_rows:
		ticker_to_asset[r["ticker_lc"]] = (r["asset_id"], r["ticker"])
		asset_id_to_ticker[int(r["asset_id"])] = r["ticker"]
		if r.get("name_lc"):
			alias_to_asset[r["name_lc"]] = (r["asset_id"], r["ticker"])

	for r in alias_rows:
		alias = r["alias_lc"].strip()
		if not alias:
			continue
		asset_id = int(r["asset_id"])
		sym = asset_id_to_ticker.get(asset_id)
		alias_tokens = alias.split()
		if len(alias_tokens) == 1:
			alias_to_asset[alias_tokens[0]] = (asset_id, sym)
			if sym and sym.lower() == alias_tokens[0]:
				aliases_equal_ticker.add(alias_tokens[0])
		else:
			first = alias_tokens[0]
			multi_alias_index.setdefault(first, []).append((alias_tokens, asset_id, sym or ""))

	for first, lst in multi_alias_index.items():
		lst.sort(key=lambda t: len(t[0]), reverse=True)

	return alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index, aliases_equal_ticker


def iter_mentions(
	text: str,
	alias_to_asset: Dict[str, Tuple[int, str | None]],
	ticker_to_asset: Dict[str, Tuple[int, str | None]],
	multi_alias_index: Dict[str, List[Tuple[List[str], int, str]]],
	aliases_equal_ticker: Set[str],
) -> Iterable[Tuple[int, str, str]]:
	lower = text.lower()
	word_pattern = r'(?:^|(?<=\s)|(?<=\W))(\$?[a-z0-9.-]+)(?=\s|(?=\W)|$)'
	lower_matches = re.findall(word_pattern, lower)
	orig_pattern = r'(?:^|(?<=\s)|(?<=\W))(\$?[A-Za-z0-9.-]+)(?=\s|(?=\W)|$)'
	orig_matches = re.findall(orig_pattern, text)
	from collections import defaultdict
	originals_by_lower = defaultdict(set)
	for ot in orig_matches:
		originals_by_lower[ot.lower()].add(ot)

	token_set: Set[str] = set(lower_matches)
	seen: Set[Tuple[int, str, str]] = set()

	for tok in token_set:
		tok_stripped = tok[1:] if tok.startswith("$") else tok
		originals = originals_by_lower.get(tok, set()) | originals_by_lower.get(tok_stripped, set())

		def is_explicit_symbol(o: str) -> bool:
			return o.startswith("$") or (o.isupper() and any(c.isalpha() for c in o))

		# Require explicit tickers for crypto (ONLY $-prefixed or ALL-CAPS)
		if tok_stripped in ticker_to_asset:
			if any(is_explicit_symbol(ot) for ot in originals):
				pair = ticker_to_asset[tok_stripped]
				mention_text = next(iter(originals)) if originals else tok
				seen.add((pair[0], pair[1] or tok_stripped.upper(), mention_text))

		# Aliases/names and multi-word phrases are intentionally ignored for crypto

	# Multi-word alias detection disabled for crypto

	for item in seen:
		yield item


def load_sources(conn: Connection) -> dict:
	with conn.cursor(row_factory=dict_row) as cur:
		cur.execute("select id, name from sources")
		rows = cur.fetchall()
	return {str(r["name"]).lower(): int(r["id"]) for r in rows}


def insert_comment(conn: Connection, asset_id: int, source_id: int, commented_at: datetime, sentiment: int, body: str, link: str) -> None:
	with conn.cursor() as cur:
		cur.execute(
			"""
			insert into comments(asset_id, source_id, commented_at, sentiment, body, link)
			values (%s, %s, %s, %s, %s, %s)
			""",
			(asset_id, source_id, commented_at, sentiment, body, link),
		)


def make_db_connection(db_url: str) -> Connection:
	conn = connect(db_url)
	conn.autocommit = True
	return conn


def ensure_db_connection(conn: Connection | None, db_url: str) -> Connection:
	if conn is None or conn.closed:
		info("db_reconnecting")
		return make_db_connection(db_url)
	try:
		with conn.cursor() as cur:
			cur.execute("SELECT 1")
	except Exception as e:
		err("db_connection_lost", error=str(e))
		info("db_reconnecting")
		return make_db_connection(db_url)
	return conn


def fetch_json(url: str, timeout: int = 10) -> dict | list | None:
	try:
		resp = requests.get(url, timeout=timeout)
		resp.raise_for_status()
		return resp.json()
	except Exception as e:
		err("http_error", url=url, error=str(e))
		return None


def process_post(
	post: dict,
	board: str,
	conn: Connection,
	openai_client: OpenAI,
	model: str,
	alias_to_asset: Dict,
	ticker_to_asset: Dict,
	multi_alias_index: Dict,
	aliases_equal_ticker: Set,
	source_name_to_id: Dict,
	asset_id_to_ticker: Dict,
) -> Tuple[int, int]:
	body_html = post.get("com") or ""
	if not body_html:
		return 0, 0
	body = clean_html_to_text(body_html)
	if not body:
		return 0, 0

	mentions = list(set(iter_mentions(body, alias_to_asset, ticker_to_asset, multi_alias_index, aliases_equal_ticker)))
	# Filter out excluded tickers (env + global blacklist)
	env_exclude = {t.strip().upper() for t in Settings().FOURCHAN_EXCLUDE_TICKERS.split(',') if t.strip()}
	combined_exclude = env_exclude | BLACKLISTED_TICKERS
	if mentions:
		filtered: List[Tuple[int, str, str]] = []
		excluded_symbols: List[str] = []
		for asset_id, sym, mention_text in mentions:
			canon = (asset_id_to_ticker.get(asset_id, sym) or "").upper()
			if canon in combined_exclude:
				excluded_symbols.append(canon)
				continue
			filtered.append((asset_id, sym, mention_text))
		if excluded_symbols:
			info("mentions_excluded", symbols=sorted(list(set(excluded_symbols))), count=len(excluded_symbols))
		mentions = filtered
	if not mentions:
		return 0, 0

	ts = int(post.get("time") or 0)
	if ts <= 0:
		return 0, 0
	created = datetime.fromtimestamp(float(ts), tz=timezone.utc)

	thread_id = int(post.get("resto") or 0) or int(post.get("no"))
	post_id = int(post.get("no"))
	link = f"https://boards.4channel.org/{board}/thread/{thread_id}#p{post_id}"
	# Source name: use the single global '4chan' source
	src_name_used = "4chan"
	source_id = source_name_to_id.get(src_name_used)
	if not source_id:
		err("unknown_source", src=src_name_used)
		return 0, 0

	# Collapse to unique assets to avoid extra LLM calls; take first mention per asset
	unique_by_asset: Dict[int, Tuple[str, str]] = {}
	for asset_id, sym, mention_text in mentions:
		if asset_id not in unique_by_asset:
			unique_by_asset[asset_id] = (sym, mention_text)

	# Perform a single LLM call per asset
	kept: List[Tuple[int, str, str]] = []
	sentiments: Dict[int, int] = {}
	sentiment_labels: Dict[int, str] = {}
	for asset_id, (sym, mention_text) in unique_by_asset.items():
		sent, sent_label = score_sentiment_llm(openai_client, model, body, sym, mention_text)
		kept.append((asset_id, sym, mention_text))
		sentiments[asset_id] = sent
		sentiment_labels[asset_id] = sent_label

	if not kept:
		return 0, 0

	inserted_asset_ids: List[int] = []
	for asset_id in {aid for (aid, _, _) in kept}:
		sent = sentiments[asset_id]
		insert_comment(conn, asset_id, source_id, created, sent, body, link)
		inserted_asset_ids.append(asset_id)

	tickers = sorted({ asset_id_to_ticker.get(aid, "") for aid in inserted_asset_ids if asset_id_to_ticker.get(aid) })
	preview = body[:220] + ("..." if len(body) > 220 else "")
	decisions = {asset_id_to_ticker.get(aid, ""): sentiment_labels.get(aid, "") for aid in inserted_asset_ids if asset_id_to_ticker.get(aid)}
	info("mention_inserted", n=len(inserted_asset_ids), tickers=tickers, decisions=decisions, src=src_name_used, link=link, preview=preview)
	return len(inserted_asset_ids), len(mentions)


def main():
	s = Settings()
	boards = [b.strip() for b in (s.FOURCHAN_BOARDS or "biz").split(',') if b.strip()]
	info("fourchan_crypto_start", boards=boards, poll_seconds=s.POLL_SECONDS)

	if not s.OPENAI_API_KEY:
		raise RuntimeError("OPENAI_API_KEY is required")
	openai_client = OpenAI(api_key=s.OPENAI_API_KEY)
	model = s.OPENAI_MODEL

	if not s.DATABASE_URL:
		raise RuntimeError("DATABASE_URL is required")
	conn = make_db_connection(s.DATABASE_URL)

	alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index, aliases_equal_ticker = fetch_alias_map(conn)
	source_name_to_id = load_sources(conn)
	last_alias_refresh = time.time()
	ALIAS_REFRESH_INTERVAL = 3600

	last_modified_by_thread: Dict[str, Dict[int, int]] = {b: {} for b in boards}
	seen_post_ids: Set[int] = set()

	info("fourchan_crypto_stream_start", boards=boards)
	while True:
		try:
			if time.time() - last_alias_refresh > ALIAS_REFRESH_INTERVAL:
				info("alias_map_reloading")
				conn = ensure_db_connection(conn, s.DATABASE_URL)
				alias_to_asset, ticker_to_asset, asset_id_to_ticker, multi_alias_index, aliases_equal_ticker = fetch_alias_map(conn)
				last_alias_refresh = time.time()
				info("alias_map_reloaded")

			for board in boards:
				threads_checked = 0
				threads_updated = 0
				posts_processed = 0
				posts_with_mentions = 0
				mentions_detected = 0
				mentions_inserted = 0
				catalog_url = f"https://a.4cdn.org/{board}/catalog.json"
				pages = fetch_json(catalog_url)
				if not isinstance(pages, list):
					info("catalog_fetch_empty", board=board)
					continue
				total_threads = sum(len(p.get("threads") or []) for p in pages)
				info("catalog_fetched", board=board, pages=len(pages), threads=total_threads)
				for page in pages:
					threads = page.get("threads") or []
					for t in threads:
						threads_checked += 1
						thread_no = int(t.get("no"))
						last_mod = int(t.get("last_modified") or 0)
						prev_mod = last_modified_by_thread[board].get(thread_no, 0)
						if last_mod <= prev_mod:
							continue
						threads_updated += 1
						info("thread_update_detected", board=board, thread=thread_no, last_modified_prev=prev_mod, last_modified_new=last_mod)
						thread_url = f"https://a.4cdn.org/{board}/thread/{thread_no}.json"
						data = fetch_json(thread_url)
						if not isinstance(data, dict):
							continue
						posts = data.get("posts") or []
						new_post_ids = [int(p.get("no") or 0) for p in posts if int(p.get("no") or 0) not in seen_post_ids]
						info("thread_fetched", board=board, thread=thread_no, posts=len(posts), new_posts=len(new_post_ids))
						cutoff_ts = int((datetime.now(timezone.utc) - timedelta(minutes=s.FOURCHAN_MAX_AGE_MINUTES)).timestamp())
						skipped_old = 0
						for p in posts:
							pid = int(p.get("no") or 0)
							if pid and pid in seen_post_ids:
								continue
							pt = int(p.get("time") or 0)
							if pt and pt < cutoff_ts:
								skipped_old += 1
								continue
							conn = ensure_db_connection(conn, s.DATABASE_URL)
							try:
								inserted_count, detected_count = process_post(
									p,
									board,
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
								posts_processed += 1
								if detected_count > 0:
									posts_with_mentions += 1
									mentions_detected += detected_count
									mentions_inserted += inserted_count
							except Exception as e:
								err("process_post_error", error=str(e), post_id=pid, board=board)
								time.sleep(1)
								continue
							if pid:
								seen_post_ids.add(pid)
						if skipped_old:
							info("skipped_old_posts", board=board, thread=thread_no, skipped=skipped_old, cutoff_minutes=s.FOURCHAN_MAX_AGE_MINUTES)
						last_modified_by_thread[board][thread_no] = last_mod
				info(
					"board_scan_complete",
					board=board,
					threads_checked=threads_checked,
					threads_updated=threads_updated,
					posts_processed=posts_processed,
					posts_with_mentions=posts_with_mentions,
					mentions_detected=mentions_detected,
					mentions_inserted=mentions_inserted,
				)
			info("sleeping", seconds=s.POLL_SECONDS)
			time.sleep(s.POLL_SECONDS)
		except Exception as e:
			err("fourchan_crypto_loop_error", error=str(e))
			time.sleep(max(10, s.POLL_SECONDS))


if __name__ == "__main__":
	main()
