import asyncio
import aiohttp
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import json
import re

import asyncpg
from tenacity import retry, wait_exponential, stop_after_attempt

from core.deepseek import deepseek_chat_async

from core.config import Settings
from core.logging import info, err


class StockTwitsScraper:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db_pool = None  # Will be initialized async
        
        # Active symbols (top 1500 by recent mentions)
        self.active_symbols = []
        
        # DeepSeek API key
        self.deepseek_api_key = settings.DEEPSEEK_API_KEY
        
        self.source_id = None  # Will be loaded async
        
    async def _init_db(self):
        """Initialize database connection pool."""
        self.db_pool = await asyncpg.create_pool(self.settings.DATABASE_URL, min_size=5, max_size=20)
        
        # Get StockTwits source ID
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT id FROM sources WHERE LOWER(name) = 'stocktwits'")
            if not row:
                raise RuntimeError("StockTwits source not found in database")
            self.source_id = int(row["id"])
    
    async def _load_active_symbols(self):
        """Load top 1500 most mentioned stocks from last 5 days."""
        async with self.db_pool.acquire() as conn:
            # Get top 1500 symbols by comment count in last 5 days
            results = await conn.fetch("""
                SELECT 
                    a.ticker,
                    COUNT(c.id) as comment_count_5d
                FROM assets a
                LEFT JOIN comments c ON a.id = c.asset_id 
                    AND c.commented_at >= NOW() - INTERVAL '5 days'
                WHERE a.universe = 'stock'
                GROUP BY a.ticker, a.id
                ORDER BY comment_count_5d DESC NULLS LAST
                LIMIT 1500
            """)
            
        # Extract just the tickers
        self.active_symbols = [row["ticker"] for row in results]
        
        info("active_symbols_loaded", 
             total_symbols=len(self.active_symbols),
             with_recent_activity=len([s for s in results if s["comment_count_5d"] > 0]),
             tier1_sample=self.active_symbols[:5],  # Show first 5 symbols for debugging
             service="stocktwits")
    
    async def _get_cursor_for_symbol(self, symbol: str) -> Optional[int]:
        """Get last cursor position for symbol."""
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT last_cursor_max 
                FROM stocktwits_cursors 
                WHERE symbol = $1
            """, symbol)
            return int(row["last_cursor_max"]) if row else None
    
    async def _update_cursor_for_symbol(self, symbol: str, cursor_max: int):
        """Update cursor position for symbol."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO stocktwits_cursors (symbol, last_cursor_max, last_scraped_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (symbol) 
                DO UPDATE SET 
                    last_cursor_max = EXCLUDED.last_cursor_max,
                    last_scraped_at = EXCLUDED.last_scraped_at
            """, symbol, cursor_max)
    
    async def _fetch_symbol_messages(self, session: aiohttp.ClientSession, symbol: str, headers: Dict[str, str]) -> Optional[Dict]:
        """Fetch messages for a single symbol."""
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
        
        # Add cursor if we have one
        last_cursor = await self._get_cursor_for_symbol(symbol)
        if last_cursor:
            url += f"?since={last_cursor}"
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limited
                    err("stocktwits_rate_limited", symbol=symbol)
                    await asyncio.sleep(60)  # Wait 1 minute
                    return None
                else:
                    err("stocktwits_api_error", symbol=symbol, status=response.status)
                    return None
        except Exception as e:
            err("stocktwits_fetch_error", symbol=symbol, error=str(e))
            return None
    
    async def score_sentiment_llm(self, symbol: str, message_text: str) -> int:
        """Use DeepSeek to score sentiment when StockTwits doesn't provide it."""
        try:
            # Clean and prepare the message text
            cleaned_text = re.sub(r'[^\w\s$@#.]', ' ', message_text)
            if len(cleaned_text.strip()) < 3:
                return 0  # Neutral for very short messages
            
            # Minimal sentiment classification prompt
            system_prompt = """Classify sentiment for the stock mentioned in the comment.
 Return exactly one word:
 Bullish (positive, buy, up)
 Bearish (negative, sell, down)
 Neutral (unclear, mixed)"""

            user_prompt = f"SYMBOL: {symbol}\nMESSAGE: {cleaned_text[:500]}"  # Limit length

            decision = (await deepseek_chat_async(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                api_key=self.deepseek_api_key,
                temperature=0,
                max_tokens=4,
            )).strip().lower()
            
            # Map LLM response to sentiment score (all are guaranteed stocks)
            if "bullish" in decision or "bull" in decision:
                return 1
            elif "bearish" in decision or "bear" in decision:
                return -1
            else:
                return 0  # Neutral
                
        except Exception as e:
            err("llm_sentiment_error", symbol=symbol, error=str(e))
            return 0  # Default to neutral on error
    
    async def _insert_message(self, message: Dict, symbol_info: Dict):
        """Insert a StockTwits message into the database."""
        # Extract message data
        message_id = str(message["id"])
        body = message.get("body", "")
        created_at_str = message.get("created_at", "")
        
        # Parse timestamp
        try:
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        except:
            created_at = datetime.now(timezone.utc)
        
        # Get sentiment from StockTwits API (their built-in sentiment)
        sentiment_raw = message.get("entities", {}).get("sentiment", {})
        sentiment_basic = sentiment_raw.get("basic") if sentiment_raw else None
        
        # Map to our sentiment scale
        sentiment_source = "stocktwits"
        if sentiment_basic:
            # Use StockTwits' built-in sentiment
            if sentiment_basic.lower() == "bullish":
                sentiment_score = 1
            elif sentiment_basic.lower() == "bearish":
                sentiment_score = -1
            else:
                sentiment_score = 0
        else:
            # Fall back to DeepSeek sentiment analysis
            sentiment_source = "deepseek"
            sentiment_score = await self.score_sentiment_llm(symbol_info["symbol"], body)
            
            # Skip if LLM fails (but continue processing)
            if sentiment_score is None:
                sentiment_score = 0  # Default to neutral
        
        # Get asset_id for the symbol and insert into database
        async with self.db_pool.acquire() as conn:
            asset_row = await conn.fetchrow("""
                SELECT id FROM assets 
                WHERE LOWER(ticker) = LOWER($1) AND universe = 'stock'
            """, symbol_info["symbol"])
            
            if not asset_row:
                return None  # Skip if we don't have this asset
            
            asset_id = int(asset_row["id"])
            link = f"https://stocktwits.com/symbol/{symbol_info['symbol']}"
            
            # Insert into database
            await conn.execute("""
                INSERT INTO comments (
                    asset_id, source_id, commented_at, sentiment, body, link, external_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (external_id) DO NOTHING
            """, asset_id, self.source_id, created_at, sentiment_score, 
                body, link, f"stocktwits_{message_id}")
            
        # Return sentiment source for aggregation
        return sentiment_source
    
    async def _process_symbol_batch(self, symbols: List[str], batch_name: str):
        """Process a batch of symbols concurrently."""
        if not symbols:
            return
        
        start_time = time.time()
        info("batch_start", batch=batch_name, symbols=len(symbols))
            
        # Rotate user agents to avoid rate limiting
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
        
        # Use connection pooling for better performance
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Create tasks with rotating headers
            tasks = []
            for i, symbol in enumerate(symbols):
                headers = {
                    'User-Agent': user_agents[i % len(user_agents)],
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                tasks.append(self._fetch_symbol_messages(session, symbol, headers))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Track per-symbol stats
            symbol_stats = {}
            total_messages = 0
            
            for symbol, result in zip(symbols, results):
                symbol_stats[symbol] = 0  # Initialize count
                
                if isinstance(result, Exception):
                    err("symbol_processing_error", symbol=symbol, error=str(result))
                    continue
                    
                if not result:
                    continue
                
                try:
                    messages = result.get("messages", [])
                    cursor = result.get("cursor", {})
                    symbol_info = result.get("symbol", {})
                    
                    # Process each message async
                    msg_start = time.time()
                    if messages:
                        # Process all messages for this symbol concurrently
                        insert_tasks = [self._insert_message(message, symbol_info) for message in messages]
                        sentiment_sources = await asyncio.gather(*insert_tasks, return_exceptions=True)
                        
                        # Count sentiment sources
                        stocktwits_count = sum(1 for s in sentiment_sources if s == "stocktwits")
                        deepseek_count = sum(1 for s in sentiment_sources if s == "deepseek")
                        error_count = sum(1 for s in sentiment_sources if isinstance(s, Exception))
                        
                        symbol_stats[symbol] = {
                            "total": len(messages),
                            "stocktwits_sentiment": stocktwits_count,
                            "deepseek_sentiment": deepseek_count,
                            "errors": error_count
                        }
                        total_messages += len(messages)
                        
                        msg_time = time.time() - msg_start
                        info("symbol_complete", symbol=symbol, 
                             messages=len(messages), 
                             stocktwits_sentiment=stocktwits_count,
                             deepseek_sentiment=deepseek_count,
                             processing_time_sec=round(msg_time, 2))
                    
                    # Update cursor
                    if cursor and cursor.get("max"):
                        await self._update_cursor_for_symbol(symbol, cursor["max"])
                    
                    # Initialize empty stats if no messages processed
                    if symbol not in symbol_stats:
                        symbol_stats[symbol] = {"total": 0, "stocktwits_sentiment": 0, "deepseek_sentiment": 0, "errors": 0}
                        
                except Exception as e:
                    err("symbol_error", symbol=symbol, error=str(e))
                    symbol_stats[symbol] = {"total": 0, "stocktwits_sentiment": 0, "deepseek_sentiment": 0, "errors": 1}
            
            # Calculate aggregate stats
            elapsed_time = time.time() - start_time
            total_stocktwits = sum(stats["stocktwits_sentiment"] for stats in symbol_stats.values())
            total_deepseek = sum(stats["deepseek_sentiment"] for stats in symbol_stats.values()) 
            total_errors = sum(stats["errors"] for stats in symbol_stats.values())
            symbols_with_messages = len([k for k, v in symbol_stats.items() if v["total"] > 0])
            
            info("batch_complete", 
                 batch=batch_name,
                 symbols_processed=len(symbols),
                 symbols_with_messages=symbols_with_messages,
                 total_messages=total_messages,
                 stocktwits_sentiment=total_stocktwits,
                 deepseek_sentiment=total_deepseek,
                 errors=total_errors,
                 elapsed_seconds=round(elapsed_time, 2))
    
    async def run_scraping_cycle(self):
        """Scrape all 1500 active symbols every 30 minutes."""
        current_time = datetime.now()
        
        # Run every 30 minutes (at :00 and :30)
        if current_time.minute % 30 == 0:
            # Process in batches of 50 symbols (1500 symbols / 50 = 30 batches)
            batch_size = 50
            tasks = []
            
            for i in range(0, len(self.active_symbols), batch_size):
                chunk = self.active_symbols[i:i+batch_size]
                batch_name = f"active_batch_{i//batch_size + 1}"
                tasks.append(self._process_symbol_batch(chunk, batch_name))
            
            # Execute all batches concurrently
            if tasks:
                await asyncio.gather(*tasks)
    
    async def run(self):
        """Main scraper loop."""
        info("stocktwits_scraper_starting", service="stocktwits")
        
        # Initialize database connection pool
        await self._init_db()
        
        # Load active symbols
        await self._load_active_symbols()
        
        # Ensure cursor table exists
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stocktwits_cursors (
                    symbol VARCHAR(10) PRIMARY KEY,
                    last_cursor_max BIGINT,
                    last_scraped_at TIMESTAMP DEFAULT NOW()
                )
            """)
        
        # Main loop - run every minute
        while True:
            try:
                await self.run_scraping_cycle()
                await asyncio.sleep(60)  # Wait 1 minute between cycles
                
            except Exception as e:
                err("scraping_cycle_error", error=str(e))
                await asyncio.sleep(60)


def main():
    s = Settings()
    scraper = StockTwitsScraper(s)
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()

