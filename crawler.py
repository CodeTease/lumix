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

# --- NÂNG CẤP THƯ VIỆN ---
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


class RobotsCache:
    """Quản lý việc cache file robots.txt."""
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
        
        self.visited_urls: Set[str] = set()
        self.url_queue: deque = deque()
        self.results: List[Dict] = []
        
        self.robots_cache = RobotsCache()
        self.domain_last_request: Dict[str, float] = defaultdict(float)
        self.domain_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.session: Optional[aiohttp.ClientSession] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.minio_client: Optional[Minio] = None
        
        # Tải model FastText
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
        """Tải model FastText khi crawler khởi động."""
        model_path = "/app/lid.176.bin"
        if not os.path.exists(model_path):
            console.print(f"[bold red]Lỗi: Model nhận diện ngôn ngữ '{model_path}' không tồn tại.[/bold red]")
            return None
        try:
            detector = fasttext.load_model(model_path)
            console.print("[green]✓ Model FastText cho Crawler đã được tải.[/green]")
            return detector
        except Exception as e:
            console.print(f"[red]Không thể tải model FastText: {e}[/red]")
            return None

    async def _init_database(self):
        """Khởi tạo kết nối DB Pool và đảm bảo schema được áp dụng."""
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
                 console.print(f"[bold red]Lỗi: Không tìm thấy schema.sql tại {schema_path}.[/bold red]")
                 self.save_to_db = False

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
        """Ghi lô dữ liệu pages và links vào database."""
        if not self.save_to_db or not self.db_pool:
            return

        # 1. Flush Pages
        if self.db_batch:
            try:
                async with self.db_pool.acquire() as conn:
                    data_to_insert = [
                        (
                            p['url'],
                            p['title'][:512] if p['title'] else '',
                            p.get('meta_description', ''),
                            p['domain'],
                            p['depth'],
                            p.get('body_text', ''),
                            p.get('raw_html_path'),
                            p.get('language', 'unknown')
                        ) for p in self.db_batch
                    ]
                    await conn.executemany('''
                        INSERT INTO crawled_pages (url, title, meta_description, domain, depth, body_text, raw_html_path, language, tsv_document)
                        VALUES ($1, $2::varchar(512), $3, $4, $5, $6, $7, $8, to_tsvector('english', COALESCE($2, '') || ' ' || COALESCE($6, '')))
                        ON CONFLICT (url) DO UPDATE SET
                            title = EXCLUDED.title,
                            body_text = EXCLUDED.body_text,
                            language = EXCLUDED.language,
                            crawled_at = CURRENT_TIMESTAMP
                    ''', data_to_insert)
                self.db_batch.clear()
            except Exception as e:
                console.print(f"[red]Error flushing pages: {e}[/red]")

        # 2. Flush Links (Graph Data) - New for Lith Ranker
        if self.link_batch:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.executemany('''
                        INSERT INTO page_links (source_url, target_url)
                        VALUES ($1, $2)
                        ON CONFLICT (source_url, target_url) DO NOTHING
                    ''', self.link_batch)
                self.stats['links_recorded'] += len(self.link_batch)
                self.link_batch.clear()
            except Exception as e:
                console.print(f"[red]Error flushing links: {e}[/red]")
        
    async def _add_to_db_batch(self, page_data: Dict, outgoing_links: List[str]):
        """Thêm dữ liệu vào lô xử lý."""
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
        async with self.domain_locks[domain]:
            last = self.domain_last_request[domain]
            wait = self.delay_per_domain - (time.time() - last)
            if wait > 0: await asyncio.sleep(wait)
            self.domain_last_request[domain] = time.time()
    
    async def _fetch_page(self, url: str) -> Optional[Tuple[str, str]]:
        domain = self._get_domain(url)
        if not await self.robots_cache.can_fetch(url, self.user_agent):
            self.stats['total_skipped'] += 1
            return None
        
        await self._wait_for_rate_limit(domain)
        
        async with self.rate_limiter:
            try:
                headers = {'User-Agent': self.user_agent}
                if not self.session: return None
                async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as response:
                    if response.status == 200 and 'text/html' in response.headers.get('Content-Type', ''):
                        return await response.text(), response.headers.get('Content-Type', '')
            except Exception:
                self.stats['total_errors'] += 1
                return None
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
                if depth < self.max_depth and norm_url not in self.visited_urls:
                    self.url_queue.append((norm_url, depth + 1))
        
        page_data = {
            'url': url,
            'title': title,
            'meta_description': meta_desc,
            'body_text': body_text,
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
            if norm_url in self.visited_urls: return
            self.visited_urls.add(norm_url)
            
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

    async def _discover_sitemap(self, domain: str):
        """Simple Sitemap Discovery (Stub for brevity)"""
        # Logic sitemap đã có ở version cũ, giữ nguyên nếu cần
        pass

    async def crawl(self):
        for url in self.start_urls:
            self.url_queue.append((url, 0))
        
        await self._init_database()
        self._init_minio_client()
        
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent})
        
        try:
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), console=console) as progress:
                task = progress.add_task("Crawling...", total=None)
                
                while self.url_queue and not self.shutdown_event.is_set():
                    if self.max_pages and self.stats['total_crawled'] >= self.max_pages: break
                    
                    batch = [self.url_queue.popleft() for _ in range(min(len(self.url_queue), 20))]
                    await asyncio.gather(*[self._crawl_url(u, d, semaphore) for u, d in batch])
                    
                    progress.update(task, description=f"Crawled: {self.stats['total_crawled']} | Links: {self.stats['links_recorded']}")
        finally:
            await self._flush_db_batch()
            if self.session: await self.session.close()
            await self._close_database()

    def print_summary(self):
        console.print(f"\n[bold green]Crawl Finished![/bold green]")
        console.print(f"Pages: {self.stats['total_crawled']}, Links Recorded: {self.stats['links_recorded']}")

def load_crawler_config() -> Dict:
    path = "/app/crawler/config.json"
    if os.path.exists(path):
        with open(path, 'r') as f: return json.load(f)
    return {}

async def main():
    load_dotenv()
    config = load_crawler_config()
    start_urls = config.get('start_urls', ['https://teaserverse.dev'])
    
    crawler = WebCrawler(start_urls, config)
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, crawler.shutdown_event.set)
        
    await crawler.crawl()
    crawler.print_summary()

if __name__ == "__main__":
    asyncio.run(main())