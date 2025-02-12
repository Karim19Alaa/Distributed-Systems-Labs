import socket
import threading
import logging
from commons.utils import fib, decode_request, contact_db_sync

# Configure logging (Best Practice)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def handle_client(client_socket):
    """Handles a single client connection in a separate thread."""
    addr = client_socket.getpeername()
    logger.info(f"Client connected: {addr}")

    try:
        request_bytes = client_socket.recv(1024)
        if not request_bytes:  # Handle client disconnection
            logger.info(f"Client disconnected: {addr}")
            client_socket.close()
            return

        request = request_bytes.decode()
        op, arg = decode_request(request)

        if op == "fib":
            try:
                result = fib(arg)
                client_socket.sendall(str(result).encode())
                logger.debug(f"Sent fibonacci result to {addr}: {result}")
            except Exception as e:
                logger.error(f"Error calculating fibonacci for {addr}: {e}")
                client_socket.sendall(b"Error calculating fibonacci")

        elif op == "contact_db":
            try:
                db_response = contact_db_sync()
                client_socket.sendall(db_response)
                logger.debug(f"Sent DB response to {addr}: {db_response}")
            except Exception as e:
                logger.error(f"Error contacting DB for {addr}: {e}")
                client_socket.sendall(b"Error contacting database")

        elif op == "invalid":
            client_socket.sendall(b"Invalid request")
            logger.warning(f"Invalid request from {addr}: {request}")

    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")
    finally:
        try:  # Handle potential socket errors during close
            client_socket.close()
            logger.info(f"Client disconnected: {addr}")
        except Exception as e:
            logger.error(f"Error closing socket for {addr}: {e}")


def run_server(host, port):
    """Runs the server, handling each client in a separate thread."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reuse of address
    server_socket.bind((host, port))
    server_socket.listen(5)
    logger.info(f"Server started on {host}:{port}")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            logger.debug(f"Accepted connection from {addr}")  # Log connection acceptance

            thread = threading.Thread(target=handle_client, args=(client_socket,))
            thread.daemon = True # Allow main thread to exit even if clients are connected
            thread.start()

    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.critical(f"Server crashed: {e}")
    finally:
        server_socket.close()
        logger.info("Server stopped.")

if __name__ == "__main__":
    run_server('0.0.0.0', 8080)