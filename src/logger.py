import logging
from rich.console import Console
from rich.logging import RichHandler


class RichConsole:
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
            self.status.stop()

        self.status = self.console.status(self.status_message, spinner="bouncingBar")
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

    def log(self, output):
        self.pause_status()
        self.logger.log(output, extra={"markup": True})
        self.resume_status()

    def info(self, output):
        self.pause_status()
        self.logger.info(output, extra={"markup": True})
        self.resume_status()

    def error(self, output):
        self.pause_status()
        self.logger.error(output, extra={"markup": True})
        self.resume_status()

    def warning(self, output):
        self.pause_status()
        self.logger.warning(output, extra={"markup": True})
        self.resume_status()


# Global console
console = RichConsole()
