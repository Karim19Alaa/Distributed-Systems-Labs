import socket
import selectors
import logging
from commons.utils import fib, decode_request, contact_db_sync

# Configure logging (Best Practice)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sel = selectors.DefaultSelector()

def handle_client(sock):
    """Handles a single client connection."""
    addr = sock.getpeername()
    logger.info(f"Client connected: {addr}")

    try:
        request_bytes = sock.recv(1024)
        if not request_bytes:  # Handle client disconnection
            logger.info(f"Client disconnected: {addr}")
            sel.unregister(sock)
            sock.close()
            return

        request = request_bytes.decode()
        op, arg = decode_request(request)

        if op == "fib":
            try:
                result = fib(arg)
                sock.sendall(str(result).encode())
                logger.debug(f"Sent fibonacci result to {addr}: {result}")
            except Exception as e:
                logger.error(f"Error calculating fibonacci for {addr}: {e}")
                sock.sendall(b"Error calculating fibonacci")

        elif op == "contact_db":
            try:
                db_response = contact_db_sync()
                sock.sendall(db_response)
                logger.debug(f"Sent DB response to {addr}: {db_response}")
            except Exception as e:
                logger.error(f"Error contacting DB for {addr}: {e}")
                sock.sendall(b"Error contacting database")

        elif op == "invalid":
            sock.sendall(b"Invalid request")
            logger.warning(f"Invalid request from {addr}: {request}")

    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")
    finally:
        try: # Handle potential socket errors during close
            sock.close()
            logger.info(f"Client disconnected: {addr}")
        except Exception as e:
            logger.error(f"Error closing socket for {addr}: {e}")


def accept(sock):
    """Accepts a new connection."""
    try:
        client_sock, addr = sock.accept()
        logger.info(f'Connection from {addr}')
        client_sock.setblocking(False) # Important for non-blocking
        sel.register(client_sock, selectors.EVENT_READ, handle_client)
    except Exception as e:
        logger.error(f"Error accepting connection: {e}")

def run_server(host, port):
    """Runs the server."""
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen()
    sock.setblocking(False) # Important for non-blocking
    sel.register(sock, selectors.EVENT_READ, accept)
    logger.info(f"Server started on {host}:{port}") # Log server start

    try:
        while True:
            events = sel.select()
            for key, _ in events:
                callback = key.data
                sock = key.fileobj
                callback(sock)
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.critical(f"Server crashed: {e}")
    finally:
        for key in sel.get_keys():
            sock = key.fileobj
            sel.unregister(sock)
            try:
                sock.close()
            except Exception as e:
                logger.error(f"Error closing socket: {e}")
        sel.close()
        logger.info("Server stopped.")



if __name__ == '__main__':
    run_server('0.0.0.0', 8080)