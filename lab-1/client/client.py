import asyncio
import time
import argparse
import logging
from urllib.parse import urlencode
from http.client import HTTPConnection


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def make_request(host, port, message, server_name):
    try:
        params = {'n': message}
        encoded_params = urlencode(params)
        url = f"/?{encoded_params}"

        conn = HTTPConnection(host, port)
        conn.request("GET", url)
        response = conn.getresponse()
        data = await asyncio.to_thread(response.read)
        conn.close()

        return data.decode(), server_name
    except Exception as e:
        logger.error(f"Connection error to {host}:{port}: {e}")
        return f"Error: {e}", server_name

async def run_tasks(host, port, num_requests, server_name, task_type):
    start_time = time.time()
    tasks = []

    if task_type == "fib":
        tasks = [make_request(host, port, f"fib {i}", server_name) for i in range(1, num_requests + 1)]
    elif task_type == "io":
        tasks = [make_request(host, port, "contact_db", server_name) for _ in range(num_requests)]
    else:
        logger.warning("Invalid task type.")
        return

    results = await asyncio.gather(*tasks)

    end_time = time.time()
    duration = end_time - start_time

    logger.info(f"http://{host}:{port} ({server_name} - {task_type}): {num_requests} requests in {duration:.2f} seconds")  # Use logger.info

    for data, server in results:
        logger.info(f"Server: {server}, Task: {task_type} Result: {data}")  # Use logger.info


async def main():
    parser = argparse.ArgumentParser(description="Client for concurrent server testing.")
    parser.add_argument("--num_requests", type=int, default=1, help="Number of requests to send to each server.")
    args = parser.parse_args()

    server_configs = [
        ("sync_sequential", 8081, "Sync Sequential"),
        ("sync_threaded", 8082, "Sync Threaded"),
        ("sync_threadpool", 8083, "Sync Threadpool"),
        ("async_io_multiplexing", 8084, "Async IO Multiplexing"),
        ("async_asyncio", 8085, "Async Asyncio"),
    ]
    for host, port, server_name in server_configs:
        await asyncio.gather(
            run_tasks(host, port, args.num_requests, server_name, "fib"),
            run_tasks(host, port, args.num_requests, server_name, "io"),
        )


if __name__ == "__main__":
    asyncio.run(main())