# scrapers
One image, four containers: reddit1 (stocks), reddit2 (crypto), stocktwits, fourchan.

## Quick start
cp .env.example .env
# edit DATABASE_URL, etc.
docker compose up -d --build
docker compose logs -f reddit1

## Dev tips
- Edit any service file and rebuild: docker compose up -d --build <service>
- Stop everything: docker compose down

