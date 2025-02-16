import threading
from commons.server_base import Server
from commons.utils import contact_db_sync
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ThreadedServer(Server):
    def handle_client(self, client_socket):
        addr = client_socket.getpeername()
        logger.info(f"Client connected: {addr}")

        def client_thread(client_socket):
            try:
                request_bytes = client_socket.recv(1024)
                if not request_bytes:
                    logger.info(f"Client disconnected: {addr}")
                    client_socket.close()
                    return

                request = request_bytes.decode()
                self._handle_request(client_socket, request)

            except Exception as e:
                logger.error(f"Error handling client {addr}: {e}")
            finally:
                try:
                    client_socket.close()
                    logger.info(f"Client disconnected: {addr}")
                except Exception as e:
                    logger.error(f"Error closing socket for {addr}: {e}")

        thread = threading.Thread(target=client_thread, args=(client_socket,))
        thread.daemon = True
        thread.start()

    def contact_db_operation(self):
        return contact_db_sync()


if __name__ == "__main__":
    server = ThreadedServer("0.0.0.0", 8080)
    server.run()
