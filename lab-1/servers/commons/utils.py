import socket
import asyncio
from urllib.parse import urlparse, parse_qs
import logging


def fib(n):
    if n <= 1:
        return n
    return fib(n-1) + fib(n-2)


# Configure logging (Best Practice) - If not already configured elsewhere
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def decode_request(request_line):
    """Decodes a request line to extract the request type and value.

    Args:
        request_line: The HTTP request line (e.g., "GET /?n=fib%2010 HTTP/1.1").

    Returns:
        A tuple (request_type, value) or ("invalid", None) if the request is invalid.
    """
    try:

        path = request_line.split()[1]  # Extract the path (e.g., /?n=fib%2010)
        parsed_url = urlparse(path)

        query_params = parse_qs(parsed_url.query)
        message = query_params.get('n', [None])[0] 

        if message:
            if message.startswith("fib"):
                try:
                    n = int(message.split()[1])
                    return "fib", n
                except (IndexError, ValueError):
                    logger.warning(f"Invalid fibonacci request: {message}")
                    return "invalid", None
            elif message == "contact_db":
                return "contact_db", None
            else:
                logger.warning(f"Invalid request type: {message}")
                return "invalid", None
        else:
            logger.warning("Missing 'n' parameter in request.") 
            return "invalid", None

    except (IndexError, AttributeError):
        logger.error(f"Invalid request line format: {request_line}")
        return "invalid", None
    except Exception as e:
        logger.exception(f"Unexpected error during request decoding: {e}")
        return "invalid", None


async def contact_db():
    """Asynchronously contacts the database server.

    Returns:
        The database response (bytes) or an error message (bytes).
    """
    try:
        reader_db, writer_db = await asyncio.open_connection('db_server', 8080)
        db_response = await reader_db.read(1024)
        writer_db.close()
        await writer_db.wait_closed()
        logger.info("Successfully retrieved data from the database.")
        return db_response
    except ConnectionRefusedError:
        logger.error("Connection refused by the database server.") 
        return b"Error connecting to DB server: Connection refused"
    except OSError as e:
        logger.error(f"OS Error connecting to database server: {e}")
        return b"Error connecting to DB server: OS Error"
    except Exception as e:  
        logger.exception(f"Unexpected error during database connection: {e}")
        return b"Error connecting to DB server: Unexpected error"


def contact_db_sync():
    """Synchronously contacts the database server.

    Returns:
        The database response (bytes) or an error message (bytes).
    """
    try:
        with socket.create_connection(('db_server', 8080)) as db_socket:
            db_response = db_socket.recv(1024)
            logger.info("Successfully retrieved data from the database (sync).")
            return db_response
    except ConnectionRefusedError:
        logger.error("Connection refused by the database server (sync).")
        return b"Error connecting to DB server: Connection refused"
    except OSError as e:
        logger.error(f"OS Error connecting to database server (sync): {e}")
        return b"Error connecting to DB server: OS Error"
    except Exception as e:
        logger.exception(f"Unexpected error during database connection (sync): {e}")
        return b"Error connecting to DB server: Unexpected error"