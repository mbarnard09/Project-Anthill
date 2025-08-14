# scrapers
One image, four containers: reddit1 (stocks), reddit2 (crypto), stocktwits, fourchan.

## Reddit1 service

- Configure subreddits in `config/reddit1.subreddits.yml`:

```yaml
subreddits:
  - wallstreetbets
  - stocks
  - investing
  - pennystocks
```

- Required env (in `.env` or DO App env):
  - `DATABASE_URL=postgres://user:pass@host:5432/db` (same DB as Project Ape)
  - `REDDIT_CLIENT_ID=...`
  - `REDDIT_CLIENT_SECRET=...` (set empty for installed app)
  - `REDDIT_REFRESH_TOKEN=...`
  - optional: `REDDIT_USER_AGENT=anthill-scraper/0.1`
  - `OPENAI_API_KEY=...`
  - optional: `OPENAI_MODEL=gpt-4o-mini`

## Quick start
cp .env.example .env
# edit DATABASE_URL, etc.
docker compose up -d --build
docker compose logs -f reddit1

## Dev tips
- Edit any service file and rebuild: docker compose up -d --build <service>
- Stop everything: docker compose down

