import socket
import selectors
import logging
from commons.server_base import Server
from commons.utils import fib, decode_request

logger = logging.getLogger(__name__)


class MultiplexingServer(Server):
    def __init__(self, host, port):
        super().__init__(host, port)
        self.sel = selectors.DefaultSelector()

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
            finally:
                self.sel.unregister(client_socket)
                client_socket.close()

        elif op == "contact_db":
            try:
                self.contact_db_operation(client_socket)
            except Exception as e:
                logger.error(f"Error contacting DB: {e}")
                client_socket.sendall(b"Error contacting database")
                self.sel.unregister(client_socket)
                client_socket.close()
        else:
            client_socket.sendall(
                b"Invalid request, only 'fib' and 'contact_db' are supported"
            )
            logger.warning(f"Invalid request: {request}")
            self.sel.unregister(client_socket)
            client_socket.close()

    def handle_client(self, sock):
        addr = sock.getpeername()
        logger.info(f"Client connected: {addr}")

        try:
            request_bytes = sock.recv(1024)
            if not request_bytes:
                logger.info(f"Client disconnected: {addr}")
                self.sel.unregister(sock)
                sock.close()
                return

            request = request_bytes.decode()
            self._handle_request(sock, request)

        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
            try:
                self.sel.unregister(sock)
            except Exception:
                pass
            sock.close()

    def accept(self, sock):
        try:
            client_sock, addr = sock.accept()
            logger.info(f"Connection from {addr}")
            client_sock.setblocking(False)  # Non-blocking is crucial
            self.sel.register(client_sock, selectors.EVENT_READ, self.handle_client)
        except Exception as e:
            logger.error(f"Error accepting connection: {e}")

    def run(self):
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        self.sel.register(self.server_socket, selectors.EVENT_READ, self.accept)
        logger.info(f"Server started on {self.host}:{self.port}")

        try:
            while True:
                events = self.sel.select()
                for key, _ in events:
                    callback = key.data
                    sock = key.fileobj
                    callback(sock)
        except KeyboardInterrupt:
            logger.info("Server shutting down...")
        except Exception as e:
            logger.critical(f"Server crashed: {e}")
        finally:
            for key in list(self.sel.get_map().values()):
                sock = key.fileobj
                try:
                    self.sel.unregister(sock)
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception as e:
                    logger.error(f"Error closing socket: {e}")
            self.sel.close()
            if self.server_socket:
                self.server_socket.close()
            logger.info("Server stopped.")

    def contact_db_operation(self, client_socket):
        """
        Establish a non-blocking connection to the DB server and arrange
        for its response to be sent back to the given client_socket.
        """
        db_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        db_sock.setblocking(False)
        db_sock.connect_ex(('db_server', 8080))
        logger.debug(
            f"Initiated non-blocking DB connection for {client_socket.getpeername()}"
        )

        # When the DB socket is writable, the connection is established.
        def on_db_write(s):
            # Switch to waiting for the DB response.
            self.sel.modify(s, selectors.EVENT_READ, on_db_read)

        def on_db_read(s):
            data = s.recv(1024)
            client_socket.sendall(data)
            self.sel.unregister(s)
            s.close()
            self.sel.unregister(client_socket)
            client_socket.close()

        self.sel.register(db_sock, selectors.EVENT_WRITE, on_db_write)

if __name__ == "__main__":
    server = MultiplexingServer("0.0.0.0", 8080)
    server.run()
