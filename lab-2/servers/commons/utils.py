import socket
import asyncio
import os
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
    """
    Decodes a plain text request to extract the operation and argument.
    Expected formats:
      - "fib 10"        -> returns ("fib", 10)
      - "contact_db"    -> returns ("contact_db", None)
    """
    try:
        parts = request_line.strip().split()
        if not parts:
            return "invalid", None

        if parts[0] == "fib":
            if len(parts) == 2 and parts[1].isdigit():
                return "fib", int(parts[1])
            else:
                return "invalid", None
        elif parts[0] == "contact_db":
            return "contact_db", None
        else:
            return "invalid", None
    except Exception as e:
        logging.exception(f"Error decoding request: {request_line} - {e}")
        return "invalid", None


async def contact_db():
    """Asynchronously contacts the database server.

    Returns:
        The database response (bytes) or an error message (bytes).
    """
    try:
        reader_db, writer_db = await asyncio.open_connection(os.environ['DB_IP'], os.environ['DB_PORT'])
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