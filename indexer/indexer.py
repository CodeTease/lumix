import os
import asyncio
import asyncpg
import meilisearch.aio
from dotenv import load_dotenv
import logging
import signal
from minio import Minio
from bs4 import BeautifulSoup
import io
import time

# --- 1. Logging configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Load environment variables ---
load_dotenv()

# --- 3. Config ---
DATABASE_URL = os.getenv("DATABASE_URL")
MEILI_HOST = os.getenv("MEILI_HOST")
MEILI_API_KEY = os.getenv("MEILI_API_KEY")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawler-data") # Default bucket if not set

INDEX_NAME = "pages"
BATCH_SIZE = 1000
SLEEP_INTERVAL = 30
CLEANUP_INTERVAL_SECONDS = 3600 * 6  # Run cleanup every 6 hours

# --- 4. Main logic ---
async def cleanup_deleted_pages(db_pool, meili_client):
    """
    Sync deletions: Remove documents from MeiliSearch that do not exist in PostgreSQL.
    Strategy: Fetch all IDs from DB (int64, memory efficient) and compare with Meili docs.
    """
    logging.info("Starting cleanup phase...")
    try:
        # 1. Fetch all IDs from DB
        async with db_pool.acquire() as conn:
            # Using set for O(1) lookup
            # Assuming id is bigint. 10M ids ~ 80MB RAM. Acceptable.
            rows = await conn.fetch("SELECT id FROM crawled_pages")
            db_ids = {r['id'] for r in rows}
        
        logging.info(f"Loaded {len(db_ids)} IDs from Database.")

        # 2. Iterate MeiliSearch documents
        # We need to scroll through all documents.
        limit = 2000
        offset = 0
        deleted_count = 0
        
        while True:
            # Fetch batch of docs, only need 'id' and 'url' (which is PK)
            # Note: We need 'url' to delete if PK is url, or 'id' to check against DB.
            # Assuming 'id' field exists in Meili doc as stored by this indexer.
            docs = await meili_client.index(INDEX_NAME).get_documents({
                'limit': limit,
                'offset': offset,
                'fields': ['id', 'url'] 
            })
            
            if not docs.results:
                break
                
            ids_to_delete = []
            for doc in docs.results:
                # doc['id'] should be integer, but coming from JSON it might be int or float or string?
                # In main loop: doc = dict(record), record['id'] is int.
                # Meili preserves types mostly.
                doc_id = doc.get('id')
                doc_url = doc.get('url')
                
                if doc_id is not None:
                    try:
                        doc_id = int(doc_id)
                        if doc_id not in db_ids:
                            ids_to_delete.append(doc_url) # Delete by PK (url)
                    except ValueError:
                        # If ID is somehow not int, maybe bad data, safe to delete? 
                        # Or skip. Let's skip to be safe.
                        pass
                else:
                    # No ID field? It's definitely not one of ours (or legacy). 
                    # If we are strict, we delete.
                    ids_to_delete.append(doc_url)

            if ids_to_delete:
                await meili_client.index(INDEX_NAME).delete_documents(ids_to_delete)
                deleted_count += len(ids_to_delete)
            
            offset += limit
            # Simple yield to event loop
            await asyncio.sleep(0.01)

        logging.info(f"Cleanup completed. Deleted {deleted_count} stale documents from MeiliSearch.")

    except Exception as e:
        logging.error(f"Error during cleanup: {e}")

async def main():
    db_pool = None
    meili_client = None
    minio_client = None

    try:
        logging.info("Connecting to PostgreSQL...")
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        
        logging.info(f"Connecting to Meilisearch at {MEILI_HOST}...")
        meili_client = meilisearch.aio.Client(url=MEILI_HOST, api_key=MEILI_API_KEY)

        # --- MinIO Connection ---
        if MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
             try:
                 logging.info(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
                 # Minio client is synchronous
                 minio_client = Minio(
                     MINIO_ENDPOINT,
                     access_key=MINIO_ACCESS_KEY,
                     secret_key=MINIO_SECRET_KEY,
                     secure=False 
                 )
             except Exception as e:
                 logging.error(f"MinIO connection error: {e}")
        else:
             logging.warning("MinIO config missing. Feature to fetch body_text will be disabled.")

        # --- Initialize Index ---
        logging.info(f"Initializing index '{INDEX_NAME}'...")
        await meili_client.create_index(INDEX_NAME, {'primaryKey': 'url'})

        logging.info("Updating Lith Rank settings for index...")
        settings = {
            'searchableAttributes': ['title', 'body_text', 'meta_description'],
            'filterableAttributes': ['domain', 'language'],
            'sortableAttributes': ['crawled_at', 'lith_score'],
            'rankingRules': [
                'words',
                'typo',
                'proximity',
                'attribute',
                'sort',
                'exactness',
                'lith_score:desc' 
            ]
        }
        await meili_client.index(INDEX_NAME).update_settings(settings)
        logging.info("Index settings updated.")

    except Exception as e:
        logging.critical(f"Initialization error: {e}")
        if db_pool: await db_pool.close()
        return

    logging.info("Starting synchronization loop...")
    loop = asyncio.get_running_loop()

    # Graceful Shutdown Setup
    stop_event = asyncio.Event()

    def signal_handler():
        logging.info("Received stop signal. Shutting down...")
        stop_event.set()

    loop.add_signal_handler(signal.SIGTERM, signal_handler)
    loop.add_signal_handler(signal.SIGINT, signal_handler)

    last_cleanup_time = 0

    while not stop_event.is_set():
        try:
            # 1. Indexing Task
            async with db_pool.acquire() as connection:
                query = f"""
                    SELECT id, url, title, meta_description, raw_html_path, domain, language, crawled_at, lith_score
                    FROM crawled_pages
                    WHERE indexed_at IS NULL OR crawled_at > indexed_at
                    LIMIT {BATCH_SIZE};
                """
                records = await connection.fetch(query)

                if records:
                    logging.info(f"Found {len(records)} pages to index.")
                    documents_batch = []
                    
                    for record in records:
                        doc = dict(record)
                        
                        # Fetch Body Text
                        body_text = ""
                        raw_path = doc.get('raw_html_path')
                        
                        if raw_path and minio_client:
                            try:
                                response = await loop.run_in_executor(
                                    None, 
                                    lambda: minio_client.get_object(MINIO_BUCKET, raw_path)
                                )
                                try:
                                    content = response.read()
                                    soup = BeautifulSoup(content, 'lxml')
                                    for script in soup(["script", "style", "nav", "footer", "header"]):
                                        script.decompose()
                                    body_text = soup.get_text(separator=' ', strip=True)[:100000]
                                finally:
                                    response.close()
                                    response.release_conn()
                            except Exception as e:
                                logging.warning(f"Error fetching MinIO for {doc['url']}: {e}")
                        
                        doc['body_text'] = body_text
                        if 'raw_html_path' in doc:
                             del doc['raw_html_path']

                        if doc.get('lith_score') is None:
                            doc['lith_score'] = 1.0
                        if doc.get('crawled_at'):
                            doc['crawled_at'] = doc['crawled_at'].timestamp()

                        documents_batch.append(doc)

                    await meili_client.index(INDEX_NAME).add_documents(documents_batch)
                    
                    indexed_ids = [record['id'] for record in records]
                    await connection.execute("UPDATE crawled_pages SET indexed_at = NOW() WHERE id = ANY($1::bigint[])", indexed_ids)
                    logging.info(f"Indexed {len(indexed_ids)} pages.")
                else:
                    # No new pages, maybe good time to cleanup or sleep
                    pass

            # 2. Cleanup Task (Periodic)
            now = time.time()
            if now - last_cleanup_time > CLEANUP_INTERVAL_SECONDS:
                await cleanup_deleted_pages(db_pool, meili_client)
                last_cleanup_time = now
            
            if not records:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=SLEEP_INTERVAL)
                except asyncio.TimeoutError:
                    pass

        except Exception as e:
            if stop_event.is_set():
                break
            logging.error(f"Loop error: {e}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=SLEEP_INTERVAL)
            except asyncio.TimeoutError:
                pass

    logging.info("Indexer stopped safely.")
    if db_pool:
        await db_pool.close()
    if meili_client:
        await meili_client.close()

if __name__ == "__main__":
    asyncio.run(main())
