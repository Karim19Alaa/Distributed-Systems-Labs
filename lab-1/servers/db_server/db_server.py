import asyncio
import logging

# Configure logging (Best Practice)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def handle_client(reader, writer):
    """Handles a single client connection.

    Args:
        reader: The asyncio.StreamReader for reading data from the client.
        writer: The asyncio.StreamWriter for writing data to the client.
    """
    addr = writer.get_extra_info('peername')  # Get client address for logging
    logger.info(f"Client connected: {addr}")

    try:
        await asyncio.sleep(3)  # Simulate some work
        writer.write(b"john doe\n")
        await writer.drain()
        logger.debug(f"Sent data to client: {addr}") # Debug logging
    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")  # Log errors with client info
    finally: # Ensure proper closure
        writer.close()
        await writer.wait_closed()
        logger.info(f"Client disconnected: {addr}")


async def main():
    """Starts the server and handles incoming connections."""
    try:
        server = await asyncio.start_server(handle_client, '0.0.0.0', 8080)
        addr = server.sockets[0].getsockname() # Get server address
        logger.info(f"Server started on {addr}")

        await server.wait_closed() # Keep server running
    except Exception as e:
        logger.critical(f"Server crashed: {e}") # Log critical errors
    finally:
        server.close() # Ensure server closure
        await server.wait_closed()
        logger.info("Server stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutting down...")