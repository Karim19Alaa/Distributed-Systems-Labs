import asyncio
import time
import argparse
import logging
import socket

# Configure logging with a clear format.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sync_make_request(host, port, message):
    """
    Synchronously send a request message to a server via a TCP socket
    and receive the response.
    """
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            sock.sendall(message.encode())
            response = sock.recv(1024)
            return response.decode()
    except Exception as e:
        return f"Error: {e}"

async def make_request(host, port, message):
    """
    Wrap the synchronous socket call in asyncio to avoid blocking.
    """
    return await asyncio.to_thread(sync_make_request, host, port, message)

async def run_tasks(host, port, num_requests, server_name, task_type):
    """
    Sends a series of requests to a server and logs detailed output.
    """
    logger.info(f"----- Starting tests for {server_name} (Task: {task_type}) -----")
    start_time = time.time()


    if task_type == "fib":
        messages = [f"fib 20" for i in range(1, num_requests + 1)]
    elif task_type == "io":
        messages = ["contact_db" for _ in range(num_requests)]
    else:
        logger.error(f"Invalid task type: {task_type}")
        return

    tasks = [make_request(host, port, msg) for msg in messages]
    results = await asyncio.gather(*tasks)

    duration = time.time() - start_time

    for idx, result in enumerate(results, 1):
        logger.info(f"{server_name} | {task_type} | Request {idx}: {result}")

    logger.info(f"----- Completed {num_requests} '{task_type}' requests for {server_name} in {duration:.2f} seconds -----\n")

async def main():
    parser = argparse.ArgumentParser(description="Client for concurrent server testing using sockets.")
    parser.add_argument("--num_requests", type=int, default=10, help="Number of requests to send per task.")
    args = parser.parse_args()

    server_configs = [
        ("localhost", 8081, "Sync Sequential"),
        ("localhost", 8082, "Sync Threaded"),
        ("localhost", 8083, "Sync Threadpool"),
        ("localhost", 8084, "Async IO Multiplexing"),
        ("localhost", 8085, "Async Asyncio"),
    ]
    for host, port, server_name in server_configs:
        await asyncio.gather(
            run_tasks(host, port, args.num_requests, server_name, "fib"),
            run_tasks(host, port, args.num_requests, server_name, "io"),
        )


if __name__ == "__main__":
    asyncio.run(main())
