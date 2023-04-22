from watchdog.events import LoggingEventHandler, FileSystemEventHandler, FileClosedEvent
import re, logging, os

logger = logging.getLogger("CPCargo")


class CheckpointHandler(FileSystemEventHandler):

  def __init__(self, src_path: str, dst_url: str, file_re: str,
               recursive: bool) -> None:
    super().__init__()
    self._dst_url = dst_url
    self._src_path = src_path
    self._file_re = re.compile(file_re)

  def on_any_event(self, event):
    logger.info("Got event {event}".format(event=event))
    return super().on_any_event(event)

  def on_closed(self, event: FileClosedEvent):
    event.is_directory
    return super().on_closed(event)
