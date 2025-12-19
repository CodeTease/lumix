import asyncio
import aiohttp
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from collections import defaultdict, deque
from datetime import datetime
import json
import time
import os
import signal
import ssl
import hashlib
import io
from typing import Set, Dict, List, Optional, Tuple
import threading

# --- LIBRARIES UPGRADE ---
from prometheus_client import start_http_server, Counter
import redis.asyncio as redis
import fasttext 
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
import asyncpg
from aiolimiter import AsyncLimiter
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

console = Console()

# --- METRICS ---
PAGES_CRAWLED = Counter('pages_crawled_total', 'Total pages crawled')
CRAWL_ERRORS = Counter('crawl_errors_total', 'Total crawl errors')


class RobotsCache:
    """Manages robots.txt caching."""
    def __init__(self):
        self.cache: Dict[str, RobotFileParser] = {}
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        if base_url not in self.cache:
            await self._load_robots(base_url, user_agent)
        
        return self.cache[base_url].can_fetch(user_agent, url)
    
    async def _load_robots(self, base_url: str, user_agent: str):
        robots_url = urljoin(base_url, "/robots.txt")
        rp = RobotFileParser()
        rp.set_url(robots_url)
        
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    content = await response.text()
                    rp.parse(content.splitlines())
                else:
                    rp.parse([])
        except Exception as e:
            console.print(f"[yellow]Warning: Could not fetch robots.txt for {base_url}: {e}[/yellow]")
            rp.parse([])
        
        self.cache[base_url] = rp
    
    async def close(self):
        if self.session:
            await self.session.close()


class WebCrawler:
    def __init__(
        self,
        start_urls: List[str],
        config: Dict
    ):
        self.start_urls = start_urls
        self.config = config
        
        # Crawler settings
        self.max_depth = config.get('max_depth', 3)
        self.max_concurrent_requests = config.get('max_concurrent_requests', 10)
        self.delay_per_domain = config.get('delay_per_domain', 1.0)
        self.user_agent = config.get('user_agent', 'TeaserBot/1.0')
        self.max_pages = config.get('max_pages', None)
        self.max_retries = config.get('max_retries', 3)
        self.save_to_db = config.get('save_to_db', True)
        self.save_to_json = config.get('save_to_json', True)
        self.ssl_verify = config.get('ssl_verify', True)
        self.db_ssl_ca_cert_path = config.get('db_ssl_ca_cert_path', None)
        
        # Batch buffers
        self.db_batch: List[Dict] = []
        self.link_batch: List[Tuple[str, str]] = [] # Store (source, target) tuples
        self.db_batch_size = config.get('db_batch_size', 1000) 
        
        # Redis Clients
        self.redis: Optional[redis.Redis] = None
        
        self.results: List[Dict] = []
        
        self.robots_cache = RobotsCache()
        # self.domain_last_request and locks removed in favor of Redis
        self.session: Optional[aiohttp.ClientSession] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.minio_client: Optional[Minio] = None
        
        # Load FastText model
        self.lang_detector = self._init_language_detector()
        
        self.rate_limiter = AsyncLimiter(config.get('max_requests_per_minute', 60), 60)
        self.shutdown_event = asyncio.Event()
        
        self.stats = {
            'total_crawled': 0,
            'total_skipped': 0,
            'total_errors': 0,
            'cdn_uploads': 0,
            'cdn_errors': 0,
            'links_recorded': 0,
            'by_domain': defaultdict(lambda: {'crawled': 0, 'skipped': 0, 'errors': 0})
        }
    
    def _init_language_detector(self):
        """Loads FastText model on crawler startup."""
        model_path = "/app/lid.176.bin"
        if not os.path.exists(model_path):
            console.print(f"[bold red]Error: Language detection model '{model_path}' does not exist.[/bold red]")
            return None
        try:
            detector = fasttext.load_model(model_path)
            console.print("[green]✓ FastText model for Crawler loaded.[/green]")
            return detector
        except Exception as e:
            console.print(f"[red]Could not load FastText model: {e}[/red]")
            return None

    async def _init_redis(self):
        """Initializes Redis connection."""
        redis_host = os.environ.get('REDIS_HOST', 'redis')
        redis_port = int(os.environ.get('REDIS_PORT', 6379))
        try:
            self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            await self.redis.ping()
            console.print("[green]✓ Connected to Redis[/green]")
        except Exception as e:
            console.print(f"[red]Error connecting to Redis: {e}[/red]")
            raise e

    async def _init_database(self):
        """Initializes DB Pool and ensures schema is applied."""
        if not self.save_to_db:
            return
        
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            console.print("[yellow]Warning: DATABASE_URL not set. Skipping database storage.[/yellow]")
            self.save_to_db = False
            return
        
        ssl_context = None
        if self.db_ssl_ca_cert_path and os.path.exists(self.db_ssl_ca_cert_path):
            ssl_context = ssl.create_default_context(cafile=self.db_ssl_ca_cert_path)
        elif not self.ssl_verify:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            ssl_context = ssl.create_default_context()

        try:
            self.db_pool = await asyncpg.create_pool(database_url, ssl=ssl_context, min_size=1, max_size=10)
            
            schema_path = '/app/crawler/schema.sql'
            if os.path.exists(schema_path):
                 with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                 async with self.db_pool.acquire() as conn:
                    await conn.execute(schema_sql)
                 console.print("[green]✓ Database initialized successfully[/green]")
            else:
                 console.print(f"[bold red]Error: schema.sql not found at {schema_path}.[/bold red]")
                 self.save_to_db = False
            
            # Apply link migration if needed
            # We assume migration_links.sql exists if we are running in the new version
            migration_path = '/app/crawler/migration_links.sql'
            if os.path.exists(migration_path):
                 with open(migration_path, 'r') as f:
                    migration_sql = f.read()
                 async with self.db_pool.acquire() as conn:
                    try:
                        await conn.execute(migration_sql)
                        console.print("[green]✓ Links migration applied successfully[/green]")
                    except Exception as e:
                        console.print(f"[yellow]Migration warning: {e}[/yellow]")

        except Exception as e:
            console.print(f"[red]Error initializing database: {e}[/red]")
            self.save_to_db = False

    def _init_minio_client(self):
        minio_config = self.config.get('minio_storage', {})
        if not minio_config.get('enabled', False):
            return

        endpoint = os.environ.get('MINIO_ENDPOINT', minio_config.get('endpoint'))
        access_key = os.environ.get('MINIO_ACCESS_KEY', minio_config.get('access_key'))
        secret_key = os.environ.get('MINIO_SECRET_KEY', minio_config.get('secret_key'))
        
        if not all([endpoint, access_key, secret_key]):
            console.print("[red]Error: MinIO config missing.[/red]")
            return
        
        try:
            self.minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=minio_config.get('secure', True)
            )
            bucket_name = minio_config.get('bucket_name')
            if bucket_name and not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
            console.print("[green]✓ MinIO client initialized[/green]")
        except Exception as e:
            console.print(f"[red]Error initializing MinIO: {e}[/red]")
            self.minio_client = None

    async def _close_database(self):
        if self.db_pool:
            await self.db_pool.close()

    def _extract_body_text(self, soup: BeautifulSoup) -> str:
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        return text[:5000]

    async def _store_raw_html_in_cdn(self, url: str, html_content: str) -> Optional[str]:
        if not self.minio_client: return None
        
        minio_config = self.config.get('minio_storage', {})
        bucket_name = minio_config.get('bucket_name')
        if not bucket_name: return None

        try:
            url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
            object_name = f"raw_html/{url_hash}.html"
            html_bytes = html_content.encode('utf-8')
            html_stream = io.BytesIO(html_bytes)
            self.minio_client.put_object(bucket_name, object_name, html_stream, len(html_bytes), content_type='text/html')
            self.stats['cdn_uploads'] += 1
            return object_name
        except Exception:
            self.stats['cdn_errors'] += 1
            return None
            
    async def _flush_db_batch(self):
        """Flushes batches of pages and links to database."""
        if not self.save_to_db or not self.db_pool:
            return

        local_db_batch = list(self.db_batch)
        self.db_batch.clear()

        local_link_batch = list(self.link_batch)
        self.link_batch.clear()

        # 1. Flush Pages
        if local_db_batch:
            try:
                async with self.db_pool.acquire() as conn:
                    # Insert pages and get IDs if needed, but here we update existing or insert new.
                    data_to_insert = [
                        (
                            p['url'],
                            p['title'][:512] if p['title'] else '',
                            p.get('meta_description', ''),
                            p['domain'],
                            p['depth'],
                            p.get('body_text', ''),
                            p.get('raw_html_path'),
                            p.get('language', 'unknown'),
                            p.get('text_length', 0)
                        ) for p in local_db_batch
                    ]
                    
                    # We need to make sure pages exist before linking
                    # This query handles upsert for crawled pages
                    await conn.executemany('''
                        INSERT INTO crawled_pages (url, title, meta_description, domain, depth, body_text, raw_html_path, language, text_length, tsv_document)
                        VALUES ($1, $2::varchar(512), $3, $4, $5, $6, $7, $8, $9, to_tsvector('english', COALESCE($2, '') || ' ' || COALESCE($6, '')))
                        ON CONFLICT (url) DO UPDATE SET
                            title = EXCLUDED.title,
                            body_text = EXCLUDED.body_text,
                            language = EXCLUDED.language,
                            text_length = EXCLUDED.text_length,
                            crawled_at = CURRENT_TIMESTAMP
                    ''', data_to_insert)
            except Exception as e:
                console.print(f"[red]Error flushing pages: {e}[/red]")

        # 2. Flush Links (Graph Data) - USING INTEGER IDs
        if local_link_batch:
            try:
                async with self.db_pool.acquire() as conn:
                    async with conn.transaction():
                        # Create temporary table for link pairs (source_url, target_url)
                        await conn.execute("CREATE TEMP TABLE temp_links (source_url text, target_url text) ON COMMIT DROP")
                        await conn.copy_records_to_table("temp_links", records=local_link_batch)

                        # Ensure target URLs exist in crawled_pages (stub records if needed)
                        # We insert targets that don't exist yet with minimal info
                        await conn.execute('''
                            INSERT INTO crawled_pages (url, domain, depth)
                            SELECT DISTINCT target_url, 
                                          split_part(split_part(target_url, '//', 2), '/', 1), 
                                          -1 -- Unknown depth
                            FROM temp_links
                            WHERE target_url NOT IN (SELECT url FROM crawled_pages)
                            ON CONFLICT (url) DO NOTHING
                        ''')

                        # Now insert into page_links using IDs lookups
                        await conn.execute('''
                            INSERT INTO page_links (source_id, target_id)
                            SELECT s.id, t.id
                            FROM temp_links l
                            JOIN crawled_pages s ON l.source_url = s.url
                            JOIN crawled_pages t ON l.target_url = t.url
                            ON CONFLICT (source_id, target_id) DO NOTHING
                        ''')

                        self.stats['links_recorded'] += len(local_link_batch)
            except Exception as e:
                console.print(f"[red]Error flushing links: {e}[/red]")
        
    async def _add_to_db_batch(self, page_data: Dict, outgoing_links: List[str]):
        """Adds data to the processing batch."""
        if not self.save_to_db: return

        self.db_batch.append(page_data)
        
        # Add links to link batch
        for target_url in outgoing_links:
            self.link_batch.append((page_data['url'], target_url))

        if len(self.db_batch) >= self.db_batch_size or len(self.link_batch) >= self.db_batch_size * 2:
            await self._flush_db_batch()
        
    def _normalize_url(self, url: str) -> str:
        url, _ = urldefrag(url)
        return url.rstrip('/')
    
    def _is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if not parsed.scheme in ['http', 'https']: return False
        if not parsed.netloc: return False
        excluded = ['.pdf', '.jpg', '.png', '.gif', '.css', '.js', '.zip', '.mp4', '.mp3']
        if any(url.lower().endswith(ext) for ext in excluded): return False
        return True
    
    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc
    
    async def _wait_for_rate_limit(self, domain: str):
        # Distributed Rate Limiting using Redis
        lock_key = f"rate_limit:{domain}"
        delay_ms = int(self.delay_per_domain * 1000)
        
        while True:
            # Try to set a key with expiry equal to the delay window.
            # If successful (NX), we "book" this slot.
            if await self.redis.set(lock_key, "1", px=delay_ms, nx=True):
                return
            
            # If failed, wait for the remaining time
            ttl = await self.redis.pttl(lock_key)
            if ttl > 0:
                await asyncio.sleep(ttl / 1000.0)
            else:
                await asyncio.sleep(0.1)

    async def _fetch_via_renderer(self, url: str) -> Optional[str]:
        renderer_url = "http://js_renderer:8888/render"
        try:
            params = {'url': url}
            async with self.session.get(renderer_url, params=params, timeout=aiohttp.ClientTimeout(total=60)) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            console.print(f"[red]Renderer error for {url}: {e}[/red]")
        return None

    def _is_content_insufficient(self, html: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'lxml')
            # Remove scripts and styles to check actual text
            for script in soup(["script", "style", "noscript"]):
                script.decompose()
            text = soup.get_text(separator=' ', strip=True)
            return len(text) < 100
        except Exception:
            return False

    async def _fetch_page(self, url: str) -> Optional[Tuple[str, str]]:
        domain = self._get_domain(url)
        if not await self.robots_cache.can_fetch(url, self.user_agent):
            self.stats['total_skipped'] += 1
            return None
        
        await self._wait_for_rate_limit(domain)
        
        async with self.rate_limiter:
            # Let exceptions propagate to _crawl_url for retry logic
            headers = {'User-Agent': self.user_agent}
            if not self.session: return None
            async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as response:
                if response.status == 200 and 'text/html' in response.headers.get('Content-Type', ''):
                    html = await response.text()
                    content_type = response.headers.get('Content-Type', '')
                    
                    # Fallback to JS Renderer if content is insufficient
                    if self._is_content_insufficient(html):
                        console.print(f"[yellow]SPA detected for {url}, using JS Renderer...[/yellow]")
                        rendered_html = await self._fetch_via_renderer(url)
                        if rendered_html:
                            return rendered_html, content_type
                    
                    return html, content_type
                
                # Trigger retry for server errors
                if response.status >= 500:
                    response.raise_for_status()
                    
        return None
    
    async def _parse_page(self, url: str, html: str, depth: int, cdn_path: Optional[str]) -> Tuple[Dict, List[str]]:
        soup = BeautifulSoup(html, 'lxml')
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        
        meta_desc = ""
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta: meta_desc = meta.get('content', '').strip()
        
        body_text = self._extract_body_text(soup)

        language = 'unknown'
        if self.lang_detector and len(body_text) > 50:
            try:
                clean = body_text.replace('\n', ' ').strip()
                loop = asyncio.get_running_loop()
                pred = await loop.run_in_executor(None, self.lang_detector.predict, clean, 1)
                language = pred[0][0].replace('__label__', '')
            except Exception: pass

        links = []
        for link in soup.find_all('a', href=True):
            abs_url = urljoin(url, link.get('href', ''))
            norm_url = self._normalize_url(abs_url)
            if self._is_valid_url(norm_url):
                links.append(norm_url)
                # Add to Redis Queue if depth allows
                if depth < self.max_depth:
                    is_visited = await self.redis.sismember('visited_urls', norm_url)
                    if not is_visited:
                        await self.redis.zadd('crawl_queue', {norm_url: depth + 1})
        
        page_data = {
            'url': url,
            'title': title,
            'meta_description': meta_desc,
            'body_text': body_text,
            'text_length': len(body_text),
            'depth': depth,
            'crawled_at': datetime.utcnow().isoformat(),
            'domain': self._get_domain(url),
            'raw_html_path': cdn_path,
            'language': language
        }
        return page_data, links
    
    async def _crawl_url(self, url: str, depth: int, semaphore: asyncio.Semaphore):
        async with semaphore:
            norm_url = self._normalize_url(url)
            
            if await self.redis.sadd('visited_urls', norm_url) == 0:
                 return # Already visited
            
            try:
                res = await self._fetch_page(norm_url)
                if res:
                    html, _ = res
                    cdn_path = await self._store_raw_html_in_cdn(norm_url, html)
                    page_data, links = await self._parse_page(norm_url, html, depth, cdn_path)
                    
                    # Pass links to batching logic
                    await self._add_to_db_batch(page_data, links)
                    
                    self.results.append(page_data)
                    self.stats['total_crawled'] += 1
                    self.stats['by_domain'][self._get_domain(norm_url)]['crawled'] += 1
                    PAGES_CRAWLED.inc()
            except Exception as e:
                self.stats['total_errors'] += 1
                CRAWL_ERRORS.inc()
                # Retry Logic
                fails = await self.redis.incr(f"failures:{norm_url}")
                if fails <= self.max_retries:
                    console.print(f"[yellow]Error fetching {norm_url}: {e}. Retrying ({fails}/{self.max_retries})...[/yellow]")
                    await self.redis.srem('visited_urls', norm_url)
                    await self.redis.zadd('crawl_queue', {norm_url: depth})
                else:
                    console.print(f"[bold red]Failed {norm_url} after {fails} attempts: {e}[/bold red]")
                    await self.redis.lpush('failed_urls', norm_url)

    async def _discover_sitemap(self, domain: str):
        pass

    async def crawl(self):
        await self._init_redis()
        
        # Initialize queue with start_urls if not empty
        # If queue already exists, we might just continue? 
        # But here we add start_urls again? 
        # Better: Add only if visited_urls is empty to allow resume?
        # For now, simplistic approach: Add them.
        for url in self.start_urls:
             # Only add if not visited?
             is_visited = await self.redis.sismember('visited_urls', url)
             if not is_visited:
                 await self.redis.zadd('crawl_queue', {url: 0})
        
        await self._init_database()
        self._init_minio_client()
        
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent})
        
        try:
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), console=console) as progress:
                task = progress.add_task("Crawling...", total=None)
                
                while not self.shutdown_event.is_set():
                    if self.max_pages and self.stats['total_crawled'] >= self.max_pages: break
                    
                    batch_data = await self.redis.zpopmin('crawl_queue', 20)
                    
                    if not batch_data:
                        await asyncio.sleep(2)
                        count = await self.redis.zcard('crawl_queue')
                        if count == 0:
                             break
                        continue

                    tasks = []
                    for url, depth in batch_data:
                         tasks.append(self._crawl_url(url, int(depth), semaphore))
                    
                    if tasks:
                         await asyncio.gather(*tasks)
                    
                    progress.update(task, description=f"Crawled: {self.stats['total_crawled']} | Links: {self.stats['links_recorded']}")
        finally:
            await self._flush_db_batch()
            if self.session: await self.session.close()
            await self._close_database()
            if self.redis: await self.redis.close()

    def print_summary(self):
        console.print(f"\n[bold green]Crawl Finished![/bold green]")
        console.print(f"Pages: {self.stats['total_crawled']}, Links Recorded: {self.stats['links_recorded']}")

async def load_crawler_config_from_db() -> Dict:
    """Load config from DB, fallback to environment/defaults."""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        return {}
    
    # Simple direct connection to fetch config
    try:
        conn = await asyncpg.connect(database_url)
        val = await conn.fetchval("SELECT value FROM crawler_config WHERE key = 'default'")
        await conn.close()
        if val:
            return json.loads(val)
    except Exception as e:
        console.print(f"[yellow]Could not load config from DB: {e}[/yellow]")
    
    return {}

async def main():
    load_dotenv()

    # Start Prometheus Metrics Server
    try:
        threading.Thread(target=start_http_server, args=(8000,), daemon=True).start()
        console.print("[green]✓ Prometheus metrics server started on port 8000[/green]")
    except Exception as e:
        console.print(f"[red]Failed to start metrics server: {e}[/red]")
    
    # Default config structure
    default_config = {
        'start_urls': ['https://teaserverse.online'],
        'max_depth': 1,
        'save_to_db': True
    }
    
    # Try loading from DB
    db_config = await load_crawler_config_from_db()
    
    # Merge: DB config overrides defaults
    config = {**default_config, **db_config}
    
    start_urls = config.get('start_urls', ['https://teaserverse.online'])
    
    crawler = WebCrawler(start_urls, config)
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, crawler.shutdown_event.set)
        
    await crawler.crawl()
    crawler.print_summary()

if __name__ == "__main__":
    asyncio.run(main())
