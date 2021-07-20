import logging
from rich.logging import RichHandler

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(show_path=False, enable_link_path=False, tracebacks_extra_lines=0)
    ],
)

log = logging.getLogger("rich")
