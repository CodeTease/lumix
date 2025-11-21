import os
import asyncio
import asyncpg
import meilisearch.aio
from dotenv import load_dotenv
import logging

# --- 1. Cấu hình logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Tải biến môi trường ---
load_dotenv()

# --- 3. Config ---
DATABASE_URL = os.getenv("DATABASE_URL")
MEILI_HOST = os.getenv("MEILI_HOST")
MEILI_API_KEY = os.getenv("MEILI_API_KEY")
INDEX_NAME = "pages"
BATCH_SIZE = 1000
SLEEP_INTERVAL = 30

# --- 4. Logic chính ---
async def main():
    db_pool = None
    meili_client = None

    try:
        logging.info("Đang kết nối tới PostgreSQL...")
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        
        logging.info(f"Đang kết nối tới Meilisearch tại {MEILI_HOST}...")
        meili_client = meilisearch.aio.Client(url=MEILI_HOST, api_key=MEILI_API_KEY)

        # --- Khởi tạo Index & Cấu hình Ranking ---
        logging.info(f"Đang khởi tạo index '{INDEX_NAME}'...")
        await meili_client.create_index(INDEX_NAME, {'primaryKey': 'url'})

        logging.info("Đang cập nhật cài đặt Lith Rank cho index...")
        settings = {
            'searchableAttributes': ['title', 'body_text', 'meta_description'],
            'filterableAttributes': ['domain', 'language'],
            # QUAN TRỌNG: Thêm lith_score vào sortableAttributes
            'sortableAttributes': ['crawled_at', 'lith_score'],
            # Cấu hình Ranking Rules mặc định (Lith score cao -> lên top)
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
        logging.info("Cài đặt index đã được cập nhật với Lith Rank.")

    except Exception as e:
        logging.critical(f"Lỗi khởi tạo: {e}")
        if db_pool: await db_pool.close()
        return

    logging.info("Bắt đầu vòng lặp đồng bộ...")
    while True:
        try:
            async with db_pool.acquire() as connection:
                # Lấy thêm lith_score từ DB
                query = f"""
                    SELECT id, url, title, meta_description, body_text, domain, language, crawled_at, lith_score
                    FROM crawled_pages
                    WHERE indexed_at IS NULL OR crawled_at > indexed_at
                    LIMIT {BATCH_SIZE};
                """
                records = await connection.fetch(query)

                if not records:
                    logging.info(f"Không có trang mới. Ngủ {SLEEP_INTERVAL}s...")
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                logging.info(f"Tìm thấy {len(records)} trang cần index.")
                documents_batch = [dict(record) for record in records]

                # Xử lý dữ liệu: Đảm bảo lith_score có giá trị mặc định nếu NULL
                for doc in documents_batch:
                    if doc.get('lith_score') is None:
                        doc['lith_score'] = 1.0
                    # Convert datetime to timestamp for Meilisearch sorting if needed (optional)
                    if doc.get('crawled_at'):
                        doc['crawled_at'] = doc['crawled_at'].timestamp()

                await meili_client.index(INDEX_NAME).add_documents(documents_batch)
                
                indexed_ids = [record['id'] for record in records]
                await connection.execute("UPDATE crawled_pages SET indexed_at = NOW() WHERE id = ANY($1::bigint[])", indexed_ids)
                logging.info(f"Đã index {len(indexed_ids)} trang.")

        except Exception as e:
            logging.error(f"Lỗi vòng lặp: {e}")
            await asyncio.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())