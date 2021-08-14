import logging
from rich.console import Console
from rich.logging import RichHandler


class RichConsole:
    # Static values
    LEVEL_INFO = "INFO"
    LEVEL_ERROR = "ERROR"
    LEVEL_WARNING = "WARNING"

    def __init__(self):
        self.busy = False
        self.status = False
        self.status_message = False
        self.console = Console()
        self.rich_logging = logging.basicConfig(
            level="INFO",
            format="%(message)s",
            datefmt="[%X]",
            # console=self.console,
            handlers=[
                RichHandler(
                    show_path=False, enable_link_path=False, tracebacks_extra_lines=0
                )
            ],
        )
        self.logger = logging.getLogger("rich")

    def update_status(self, output):
        self.busy = True
        self.status_message = output

        if self.status:
            self.status.update(self.status_message)
        else:
            self.status = self.console.status(
                self.status_message, spinner="bouncingBar"
            )
            self.status.start()

    def stop_status(self):
        self.busy = False
        self.status_message = False
        if self.status:
            self.status.stop()
            self.status = False

    def pause_status(self):
        self.busy = False
        if self.status:
            self.status.stop()

    def resume_status(self):
        if self.status and self.status_message:
            self.status.start()
            self.busy = True

    def format_message(self, task="LOG", message=False, action=False):
        output = ""

        if task:
            output += f"[cyan]{task}:[/] "

        if message:
            output += f"{message} "

        if action:
            output += f"[gray]~[/] [yellow]{action}[/]"

        return output

    def print_info(self, output):
        self.logger.info(output, extra={"markup": True})

    def print_error(self, output):
        self.logger.error(output, extra={"markup": True})

    def print_warning(self, output):
        self.logger.warning(output, extra={"markup": True})

    def log(self, task="LOG", message="", **kwargs):

        # Extra args
        extra = {
            "level": RichConsole.LEVEL_INFO,
            "action": False,
            "stop_status": False,
            **kwargs,
        }
        # Format log message
        output = self.format_message(task, message, extra["action"])
        # Map print levels
        print_levels = {
            f"{RichConsole.LEVEL_INFO}": self.print_info,
            f"{RichConsole.LEVEL_ERROR}": self.print_error,
            f"{RichConsole.LEVEL_WARNING}": self.print_warning,
        }

        # Select print level
        print_level = print_levels.get(extra["level"], False)

        # Skip output logging
        if not print_level:
            return
        # Pause or stop status / progress bar
        if extra["stop_status"]:
            self.stop_status()
        else:
            self.pause_status()
        # output logging
        print_level(output)
        # Resume status / progress bar
        if not extra["stop_status"]:
            self.resume_status()

    # Aliases for console.log levels
    def info(self, task=False, message=False, **kwargs):
        self.log(task, message, level=RichConsole.LEVEL_INFO, **kwargs)

    def error(self, task=False, message=False, **kwargs):
        self.log(task, message, level=RichConsole.LEVEL_ERROR, **kwargs)

    def warning(self, task=False, message=False, **kwargs):
        self.log(task, message, level=RichConsole.LEVEL_WARNING, **kwargs)


# Global console
console = RichConsole()
