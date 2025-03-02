import asyncio
import time
import argparse
import logging
import socket
from collections import defaultdict

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def sync_make_request(host, port, message):
    """Synchronously send a request and receive the response."""
    try:
        with socket.create_connection((host, port), timeout=20_000) as sock:
            sock.sendall(message.encode())
            response = sock.recv(1024)
            return response.decode()
    except Exception as e:
        return f"Error: {e}"


async def make_request(host, port, message):
    """Wrap the synchronous socket call in asyncio."""
    return await asyncio.to_thread(sync_make_request, host, port, message)


async def run_tasks(host, port, num_requests, server_name, task_type):
    """Sends requests and gathers statistics."""
    logger.info(f"----- Starting tests for {server_name} (Task: {task_type}) -----")
    start_time = time.time()

    if task_type == "fib":
        messages = [f"fib 20" for _ in range(num_requests)]
    elif task_type == "io":
        messages = ["contact_db" for _ in range(num_requests)]
    else:
        logger.error(f"Invalid task type: {task_type}")
        return

    tasks = [make_request(host, port, msg) for msg in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    duration = time.time() - start_time

    success_count = 0
    error_count = 0
    errors = defaultdict(int)

    for result in results:
        if isinstance(result, Exception) or "error" in result.lower():
            error_count += 1
            error_message = str(result) if isinstance(result, Exception) else result
            errors[error_message] += 1
            logger.debug(f"{server_name} | {task_type} | Error: {error_message}")
        else:
            success_count += 1
            logger.debug(f"{server_name} | {task_type} | Result: {result}")

    error_rate = (error_count / num_requests) * 100 if num_requests else 0

    logger.info(
        f"----- Completed {num_requests} '{task_type}' requests for {server_name} in {duration:.2f} seconds -----"
    )
    logger.info(f"{server_name} | {task_type} | Error rate: {error_rate:.2f}%")

    if errors:
        logger.debug(f"{server_name} | {task_type} | Error Summary:")
        for error, count in errors.items():
            logger.debug(f"  - {error}: {count}")
    logger.info("")


async def main():
    parser = argparse.ArgumentParser(
        description="Client for concurrent server testing."
    )
    parser.add_argument(
        "--num_requests", type=int, default=10, help="Number of requests per task."
    )
    args = parser.parse_args()

    server_configs = [
        ("localhost", 8085, "Async Asyncio"),
    ]

    for host, port, server_name in server_configs:
        await asyncio.gather(
            run_tasks(host, port, args.num_requests, server_name, "fib"),
            run_tasks(host, port, args.num_requests, server_name, "io"),
        )


if __name__ == "__main__":
    asyncio.run(main())
