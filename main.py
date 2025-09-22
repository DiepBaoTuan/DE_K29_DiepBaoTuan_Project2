import aiohttp
import aiofiles
import asyncio
import json
import re
import pandas as pd
import time
import logging
from typing import List, Dict, Union, Tuple, Set

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

list_id = pd.read_csv(r"D:\products-0-200000.csv", header=0)
df_list_id = list_id.iloc[:, 0]
total_ids: Set[int] = set(df_list_id)

def standardize_description(html_content: str) -> str:
    if not html_content:
        return ""
    cleaned_content = re.sub(r'<script.*?>.*?</script>', '', html_content, flags=re.DOTALL)
    cleaned_content = re.sub(r'<style.*?>.*?</style>', '', cleaned_content, flags=re.DOTALL)
    cleaned_content = re.sub(r'</p>|<br/?>', '\n', cleaned_content)
    cleaned_content = re.sub(r'<[^>]+>', '', cleaned_content)
    from html import unescape
    cleaned_content = unescape(cleaned_content)
    cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()
    return cleaned_content

async def fetch_product_data(
    session: aiohttp.ClientSession,
    product_id: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = 5
) -> Union[Dict, Tuple[int, str]]:
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
    retry_delay = 1
    for attempt in range(max_retries):
        try:
            async with semaphore:
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        images = data.get("images") or []
                        product_info = {
                            "id": data.get("id"),
                            "name": data.get("name"),
                            "url_key": data.get("url_key"),
                            "price": data.get("price"),
                            "description": standardize_description(data.get("description")),
                            "images_url": [img.get("base_url") for img in images if img.get("base_url")]
                        }
                        return product_info
                    elif response.status == 404:
                        # Không retry, ghi lỗi và bỏ qua
                        return (product_id, "Not Found (404)")
                    elif response.status == 429:
                        import random
                        jitter = random.uniform(0, 1)
                        delay = min(retry_delay * 2, 10) + jitter
                        logger.warning(f"Rate limit exceeded for product {product_id}, retrying in {delay:.2f}s...")
                        await asyncio.sleep(delay)
                        retry_delay = min(retry_delay * 2, 10)
                    else:
                        error_msg = f"HTTP error {response.status}"
                        return (product_id, error_msg)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            error_msg = f"Connection error: {e}"
            return (product_id, error_msg)
    return (product_id, "Exceeded max retries due to 429")

async def save_json_file(filename: str, data: List[Dict]) -> None:
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=4))
        logger.info(f"Saved {len(data)} products to {filename}")
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")

async def save_processed_ids(filename: str, processed_ids: Set[int]) -> None:
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(list(processed_ids), ensure_ascii=False, indent=4))
        logger.info(f"Updated processed IDs log with {len(processed_ids)} entries")
    except Exception as e:
        logger.error(f"Failed to save processed IDs: {e}")

async def save_not_found_ids(filename: str, not_found_ids: Set[int]) -> None:
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(list(not_found_ids), ensure_ascii=False, indent=4))
        logger.info(f"Saved {len(not_found_ids)} not found IDs")
    except Exception as e:
        logger.error(f"Failed to save not found IDs: {e}")

async def main():
    SEMAPHORE_LIMIT = 30
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    batch_size = 1000

    processed_ids_path = "processed_ids.json"
    not_found_ids_path = "not_found_ids.json"
    failed_products_log_path = "failed_products.json"

    failed_products_log: List[Dict] = []
    processed_ids: Set[int] = set()
    not_found_ids: Set[int] = set()

    # Load processed IDs
    try:
        async with aiofiles.open(processed_ids_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            processed_ids = set(json.loads(content))
            logging.info(f"Loaded {len(processed_ids)} processed IDs from previous run")
    except (FileNotFoundError, json.JSONDecodeError):
        logging.info("No processed IDs log found, starting fresh")

    # Load not found IDs (404)
    try:
        async with aiofiles.open(not_found_ids_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            not_found_ids = set(json.loads(content))
            logging.info(f"Loaded {len(not_found_ids)} not found IDs from previous run")
    except (FileNotFoundError, json.JSONDecodeError):
        logging.info("No not found IDs log found, starting fresh")
    accumulated_products: List[Dict] = []
    file_index = 0

    async with aiohttp.ClientSession() as session:
        while len(processed_ids) + len(not_found_ids) < len(total_ids):
            remaining_ids = total_ids - processed_ids - not_found_ids
            if not remaining_ids:
                break

            logging.info(f"Remaining {len(remaining_ids)} products to process")

            current_batch = list(remaining_ids)[:batch_size]
            start_time = time.time()

            tasks = [fetch_product_data(session, pid, semaphore) for pid in current_batch]
            results = await asyncio.gather(*tasks)

            for result in results:
                if isinstance(result, tuple):
                    product_id, reason = result
                    if reason == "Not Found (404)":
                        not_found_ids.add(product_id)
                    failed_products_log.append({"product_id": product_id, "reason": reason})
                    logging.warning(f"Failed product {product_id}: {reason}")
                else:
                    accumulated_products.append(result)
                    processed_ids.add(result["id"])

            # Lưu file khi đủ batch_size sản phẩm
            while len(accumulated_products) >= batch_size:
                to_save = accumulated_products[:batch_size]
                accumulated_products = accumulated_products[batch_size:]

                filename = f"products_{file_index:03d}.json"
                await save_json_file(filename, to_save)
                await save_processed_ids(processed_ids_path, processed_ids)
                await save_not_found_ids(not_found_ids_path, not_found_ids)
                file_index += 1

            elapsed = time.time() - start_time
            logging.info(f"Batch completed in {elapsed:.2f} seconds")

            await asyncio.sleep(0.5)  # Giảm thời gian sleep để tăng tốc nếu API cho phép

    # Lưu các sản phẩm còn lại chưa đủ batch_size
    if accumulated_products:
        filename = f"products_{file_index:03d}.json"
        await save_json_file(filename, accumulated_products)
        await save_processed_ids(processed_ids_path, processed_ids)
        await save_not_found_ids(not_found_ids_path, not_found_ids)

    # Lưu log lỗi
    if failed_products_log:
        try:
            async with aiofiles.open(failed_products_log_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(failed_products_log, ensure_ascii=False, indent=4))
            logging.info(f"Saved {len(failed_products_log)} failed products to {failed_products_log_path}")
        except Exception as e:
            logging.error(f"Failed to save failed products log: {e}")

    await asyncio.sleep(0)  # Dọn dẹp event loop
def run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == "__main__":
    start = time.time()
    run()
    end = time.time()
    logger.info(f"Total runtime: {end - start:.2f} seconds")