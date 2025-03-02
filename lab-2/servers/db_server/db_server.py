import asyncio
import os
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def handle_client(reader, writer):
    """Handles a single client connection.

    Args:
        reader: The asyncio.StreamReader for reading data from the client.
        writer: The asyncio.StreamWriter for writing data to the client.
    """
    addr = writer.get_extra_info("peername")
    logger.info(f"Client connected: {addr}")

    try:
        await asyncio.sleep(15)
        writer.write(b"john doe\n")
        await writer.drain()
        logger.debug(f"Sent data to client: {addr}")
    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")
    finally:
        writer.close()
        logger.info(f"Client disconnected: {addr}")


async def main():
    """Starts the server and handles incoming connections."""
    try:
        server = await asyncio.start_server(handle_client, os.environ["DB_IP"], os.environ["DB_PORT"])
        addr = server.sockets[0].getsockname()
        logger.info(f"Server started on {addr}")
        await server.wait_closed()
    except Exception as e:
        logger.critical(f"Server crashed: {e}")
    finally:
        server.close()
        logger.info("Server stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
