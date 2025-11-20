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

# --- 3. Lấy thông tin kết nối từ biến môi trường ---
DATABASE_URL = os.getenv("DATABASE_URL")
MEILI_HOST = os.getenv("MEILI_HOST")
MEILI_API_KEY = os.getenv("MEILI_API_KEY")
INDEX_NAME = "pages"
BATCH_SIZE = 1000 # Số lượng documents xử lý mỗi lần
SLEEP_INTERVAL = 30 # Giây

# --- 4. Logic chính của Indexer ---
async def main():
    """Hàm chính chạy vòng lặp vô tận để đồng bộ dữ liệu."""

    # --- 4.1. Khởi tạo kết nối ---
    db_pool = None
    meili_client = None

    try:
        logging.info("Đang kết nối tới PostgreSQL...")
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        logging.info("Kết nối PostgreSQL thành công.")

        logging.info(f"Đang kết nối tới Meilisearch tại {MEILI_HOST}...")
        meili_client = meilisearch.aio.Client(url=MEILI_HOST, api_key=MEILI_API_KEY)
        logging.info("Kết nối Meilisearch thành công.")

        # --- 4.2. Khởi tạo Index và Cài đặt ---
        logging.info(f"Đang khởi tạo index '{INDEX_NAME}'...")
        await meili_client.create_index(INDEX_NAME, {'primaryKey': 'url'})

        logging.info("Đang cập nhật cài đặt cho index...")
        settings = {
            'searchableAttributes': ['title', 'body_text', 'meta_description'],
            'filterableAttributes': ['domain', 'language'],
            'sortableAttributes': ['crawled_at']
        }
        await meili_client.index(INDEX_NAME).update_settings(settings)
        logging.info("Cài đặt index đã được cập nhật.")

    except Exception as e:
        logging.critical(f"Lỗi nghiêm trọng khi khởi tạo: {e}")
        # Nếu không thể kết nối DB hoặc Meili, thoát luôn
        if db_pool:
            await db_pool.close()
        # Không cần đóng meili_client vì nó không có hàm close() async
        return

    # --- 4.3. Vòng lặp đồng bộ ---
    logging.info("Bắt đầu vòng lặp đồng bộ...")
    while True:
        try:
            async with db_pool.acquire() as connection:

                # B1: Lấy các trang chưa được index hoặc đã cũ
                query = f"""
                    SELECT id, url, title, meta_description, body_text, domain, language, crawled_at
                    FROM crawled_pages
                    WHERE indexed_at IS NULL OR crawled_at > indexed_at
                    LIMIT {BATCH_SIZE};
                """
                records = await connection.fetch(query)

                if not records:
                    logging.info(f"Không có trang mới cần index. Đang ngủ {SLEEP_INTERVAL} giây...")
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                logging.info(f"Tìm thấy {len(records)} trang cần index.")

                # B2: Chuyển đổi dữ liệu
                documents_batch = [dict(record) for record in records]

                # B3: Gửi dữ liệu lên Meilisearch
                logging.info(f"Đang gửi {len(documents_batch)} documents lên Meilisearch...")
                task = await meili_client.index(INDEX_NAME).add_documents(documents_batch)
                # Chờ Meilisearch xác nhận (không bắt buộc, nhưng tốt cho logging)
                # await meili_client.wait_for_task(task.task_uid)
                logging.info(f"Gửi batch thành công. Task UID: {task.task_uid}")


                # B4: Cập nhật lại Postgres
                indexed_ids = [record['id'] for record in records]
                update_query = """
                    UPDATE crawled_pages
                    SET indexed_at = NOW()
                    WHERE id = ANY($1::bigint[]);
                """
                await connection.execute(update_query, indexed_ids)
                logging.info(f"Đã cập nhật indexed_at cho {len(indexed_ids)} bản ghi trong Postgres.")

        except asyncpg.exceptions.UndefinedTableError:
             logging.warning("Bảng 'crawled_pages' chưa tồn tại hoặc cột 'indexed_at' bị thiếu. Đang chờ crawler tạo bảng...")
             await asyncio.sleep(SLEEP_INTERVAL)
        except Exception as e:
            logging.error(f"Lỗi trong vòng lặp đồng bộ: {e}")
            logging.info(f"Sẽ thử lại sau {SLEEP_INTERVAL} giây...")
            await asyncio.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
