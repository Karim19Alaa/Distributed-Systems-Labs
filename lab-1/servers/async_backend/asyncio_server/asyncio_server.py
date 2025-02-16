import asyncio
from commons.server_base import Server
from commons.utils import contact_db, decode_request, fib
import logging

logger = logging.getLogger(__name__)


class AsyncIOServer(Server):
    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        logger.info(f"Client connected: {addr}")

        try:
            request_bytes = await reader.read(1024)
            if not request_bytes:
                logger.info(f"Client disconnected: {addr}")
                writer.close()

                return

            request = request_bytes.decode()
            await self._async_handle_request(reader, writer, request)

        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
        finally:
            writer.close()

            logger.info(f"Client disconnected: {addr}")

    async def _async_handle_request(self, reader, writer, request):
        op, arg = decode_request(request)

        if op == "fib":
            try:
                result = fib(arg)
                writer.write(str(result).encode())
                await writer.drain()
                logger.debug(
                    f"Sent fibonacci result to {writer.get_extra_info('peername')}: {result}"
                )
            except Exception as e:
                logger.error(f"Error calculating fibonacci: {e}")
                writer.write(b"Error calculating fibonacci")
                await writer.drain()

        elif op == "contact_db":
            try:
                db_response = await self.contact_db_operation()
                writer.write(db_response)
                await writer.drain()
                logger.debug(
                    f"Sent DB response to {writer.get_extra_info('peername')}: {db_response}"
                )
            except Exception as e:
                logger.error(f"Error contacting DB: {e}")
                writer.write(b"Error contacting database")
                await writer.drain()

        elif op == "invalid":
            writer.write(b"Invalid request")
            await writer.drain()
            logger.warning(f"Invalid request: {request}")

    async def contact_db_operation(self):
        return await contact_db()

    async def run(self):
        try:
            server = await asyncio.start_server(
                self.handle_client, self.host, self.port
            )
            addr = server.sockets[0].getsockname()
            logger.info(f"Server started on {addr}")
            await server.wait_closed()
        except Exception as e:
            logger.critical(f"Server crashed: {e}")
        finally:
            if server:
                server.close()
            logger.info("Server stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(AsyncIOServer("0.0.0.0", 8080).run())
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
