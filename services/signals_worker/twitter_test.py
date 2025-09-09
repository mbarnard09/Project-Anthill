import os
import argparse
import requests
from requests_oauthlib import OAuth1


# Hardcoded defaults (you can replace these). CLI flags or env vars still override.
TWITTER_CONSUMER_KEY_DEFAULT = "QRQl4EZltvPRmgUJEIHuMWAN4"
TWITTER_CONSUMER_SECRET_DEFAULT = "aw60NppshUqSXCS6AmdWSBmWBPCDspw9HYlHFBzrYp5rmymyZu"
TWITTER_ACCESS_TOKEN_DEFAULT = "1516137689401204746-3JBz3tNdCQkjbs6k2v0tC1i77CqKz4"
TWITTER_ACCESS_SECRET_DEFAULT = "arbGOMAqaMJxF4HDo35dhyuLVIJ3LxSDXUKJhWJKJWbJM"


def _safe_percent_str(value):
    try:
        if value is None:
            return "N/A"
        return f"{float(value):.2f}%"
    except Exception:
        return "N/A"


def _safe_money_str(value):
    try:
        if value is None:
            return "N/A"
        return f"${float(value):.2f}"
    except Exception:
        return "N/A"


def build_tweet_text(ticker,
                     company_name,
                     summary,
                     current_price,
                     pct_change_1d,
                     pct_change_7d,
                     year_high,
                     year_low):
    """Builds a tweet within 280 characters mirroring the alert format."""
    header = f"Mention Spike Alert: ${ticker}"
    intro = f"A significant increase in social media mentions has been detected for {company_name}."

    price_bits = []
    price_bits.append(f"Price: {_safe_money_str(current_price)}")
    price_bits.append(f"1d: {_safe_percent_str(pct_change_1d)}")
    price_bits.append(f"7d: {_safe_percent_str(pct_change_7d)}")
    if year_high is not None and year_low is not None:
        price_bits.append(f"52w: {_safe_money_str(year_high)}/{_safe_money_str(year_low)}")
    price_line = " | ".join(price_bits)

    base = f"{header}\n\n{intro}\n\n{price_line}\n\n"
    summary_prefix = "ChatGPT Summary of comments: "

    max_len = 280
    available = max_len - len(base) - len(summary_prefix)
    clean_summary = (summary or "").replace("\n", " ").strip()

    if available <= 0:
        condensed_bits = [f"Price: {_safe_money_str(current_price)}", f"1d: {_safe_percent_str(pct_change_1d)}"]
        condensed = " | ".join(condensed_bits)
        base = f"{header}\n\n{intro}\n\n{condensed}\n\n"
        available = max_len - len(base) - len(summary_prefix)

    if available < 20:
        base = f"{header}\n{intro}\n"
        available = max_len - len(base) - len(summary_prefix)

    if available <= 0:
        available = 0

    if len(clean_summary) > available:
        if available <= 1:
            clean_summary = "…"
        else:
            clean_summary = clean_summary[:available - 1].rstrip() + "…"

    tweet = f"{base}{summary_prefix}{clean_summary}"
    if len(tweet) > 280:
        tweet = tweet[:279] + "…"
    return tweet


def post_tweet(text,
               consumer_key,
               consumer_secret,
               access_token,
               access_secret):
    """Posts a tweet using Twitter API v2 with OAuth 1.0a credentials."""
    url = "https://api.twitter.com/2/tweets"
    auth = OAuth1(consumer_key, consumer_secret, access_token, access_secret)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "Project-Anthill-TwitterTest/1.0"
    }
    resp = requests.post(url, json={"text": text}, headers=headers, auth=auth, timeout=10)
    return resp


def verify_credentials(consumer_key,
                       consumer_secret,
                       access_token,
                       access_secret):
    """Calls Twitter 1.1 verify_credentials to validate the tokens map to a user."""
    url = "https://api.twitter.com/1.1/account/verify_credentials.json"
    auth = OAuth1(consumer_key, consumer_secret, access_token, access_secret)
    headers = {"User-Agent": "Project-Anthill-TwitterTest/1.0"}
    return requests.get(url, headers=headers, auth=auth, timeout=10)


def main():
    parser = argparse.ArgumentParser(description="Tweet test utility for Project-Anthill alerts.")
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--company-name", dest="company_name", default="Apple Inc. ($AAPL)")
    parser.add_argument("--summary", default="This is a test summary for the mention spike alert.")
    parser.add_argument("--price", type=float, default=200.12)
    parser.add_argument("--pct1d", type=float, default=1.23)
    parser.add_argument("--pct7d", type=float, default=-2.34)
    parser.add_argument("--year-high", type=float, default=None)
    parser.add_argument("--year-low", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Print only, do not post.")
    parser.add_argument("--verify-only", action="store_true", help="Only verify credentials, do not post.")

    # Credentials: env overrides hardcoded defaults; CLI overrides both
    parser.add_argument("--consumer-key", default=os.getenv("TWITTER_CONSUMER_KEY") or TWITTER_CONSUMER_KEY_DEFAULT)
    parser.add_argument("--consumer-secret", default=os.getenv("TWITTER_CONSUMER_SECRET") or TWITTER_CONSUMER_SECRET_DEFAULT)
    parser.add_argument("--access-token", default=os.getenv("TWITTER_ACCESS_TOKEN") or TWITTER_ACCESS_TOKEN_DEFAULT)
    parser.add_argument("--access-secret", default=os.getenv("TWITTER_ACCESS_SECRET") or TWITTER_ACCESS_SECRET_DEFAULT)

    args = parser.parse_args()

    tweet = build_tweet_text(
        ticker=args.ticker,
        company_name=args.company_name,
        summary=args.summary,
        current_price=args.price,
        pct_change_1d=args.pct1d,
        pct_change_7d=args.pct7d,
        year_high=args.year_high,
        year_low=args.year_low,
    )

    print("\n--- TWEET PREVIEW ---\n")
    print(tweet)
    print("\n--- LENGTH:", len(tweet), "---\n")

    if args.verify_only:
        try:
            v = verify_credentials(
                consumer_key=args.consumer_key,
                consumer_secret=args.consumer_secret,
                access_token=args.access_token,
                access_secret=args.access_secret,
            )
            print("Verify Status:", v.status_code)
            print("Verify Response:", v.text)
        except Exception as e:
            print("Failed to verify credentials:", e)
        return

    if args.dry_run:
        print("Dry run: not posting.")
        return

    # With defaults in place, credentials should always be present unless explicitly blanked

    try:
        resp = post_tweet(
            text=tweet,
            consumer_key=args.consumer_key,
            consumer_secret=args.consumer_secret,
            access_token=args.access_token,
            access_secret=args.access_secret,
        )
        print("HTTP Status:", resp.status_code)
        print("Response:", resp.text)
    except Exception as e:
        print("Failed to post tweet:", e)


if __name__ == "__main__":
    main()


