import asyncio
import logging
from commons.utils import fib, decode_request, contact_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def handle_client(reader, writer):
    """Handles a single client connection."""
    addr = writer.get_extra_info('peername')
    logger.info(f"Client connected: {addr}")

    try:
        request_bytes = await reader.read(1024)
        request = request_bytes.decode()
        op, arg = decode_request(request)

        if op == "fib":
            try:
                result = fib(arg)
                writer.write(str(result).encode())
                logger.debug(f"Sent fibonacci result to {addr}: {result}")
            except Exception as e:
                logger.error(f"Error calculating fibonacci for {addr}: {e}")
                writer.write(b"Error calculating fibonacci")  # Send error back to the client

        elif op == "contact_db":
            try:
                db_response = await contact_db()
                writer.write(db_response)
                logger.debug(f"Sent DB response to {addr}: {db_response}")
            except Exception as e:
                logger.error(f"Error contacting DB for {addr}: {e}")
                writer.write(b"Error contacting database")  # Send error back to client

        elif op == "invalid":
            writer.write(b"Invalid request")
            logger.warning(f"Invalid request from {addr}: {request}")

    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info(f"Client disconnected: {addr}")


async def main():
    """Starts the server and handles incoming connections."""
    try:
        server = await asyncio.start_server(handle_client, '0.0.0.0', 8080)
        addr = server.sockets[0].getsockname()
        logger.info(f"Server started on {addr}")

        await server.wait_closed()
    except Exception as e:
        logger.critical(f"Server crashed: {e}")
    finally:
        server.close()
        await server.wait_closed()
        logger.info("Server stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutting down...")