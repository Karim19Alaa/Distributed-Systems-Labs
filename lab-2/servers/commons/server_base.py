import abc
import socket
import logging
from commons.utils import fib, decode_request

logger = logging.getLogger(__name__)


class Server(abc.ABC):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None

    @abc.abstractmethod
    def handle_client(self, client_socket):
        pass

    def run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        logger.info(f"Server started on {self.host}:{self.port}")

        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                logger.debug(f"Accepted connection from {addr}")
                self.handle_client(client_socket)

        except KeyboardInterrupt:
            logger.info("Server shutting down...")
        except Exception as e:
            logger.critical(f"Server crashed: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
            logger.info("Server stopped.")

    def _handle_request(self, client_socket, request):
        op, arg = decode_request(request)

        if op == "fib":
            try:
                result = fib(arg)
                client_socket.sendall(str(result).encode())
                logger.debug(
                    f"Sent fibonacci result to {client_socket.getpeername()}: {result}"
                )
            except Exception as e:
                logger.error(f"Error calculating fibonacci: {e}")
                client_socket.sendall(b"Error calculating fibonacci")

        elif op == "contact_db":
            try:
                db_response = self.contact_db_operation()
                client_socket.sendall(db_response)
                logger.debug(
                    f"Sent DB response to {client_socket.getpeername()}: {db_response}"
                )
            except Exception as e:
                logger.error(f"Error contacting DB: {e}")
                client_socket.sendall(b"Error contacting database")

        else:
            client_socket.sendall(
                b"Invalid request, only 'fib' and 'contact_db' are supported"
            )
            logger.warning(f"Invalid request: {request}")

    @abc.abstractmethod
    def contact_db_operation(self):
        pass
