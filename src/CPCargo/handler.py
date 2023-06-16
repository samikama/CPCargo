from watchdog.events import FileSystemEventHandler, FileClosedEvent
from CPCargo.uploaders import Uploader
import regex, logging, os

logger = logging.getLogger("CPCargo")


class CheckpointHandler(FileSystemEventHandler):

  def __init__(self, uploader: Uploader, file_regex: str,
               recursive: bool) -> None:
    self._re = regex.compile(file_regex)
    self._uploader = uploader
    self._updated = dict()
    super().__init__()

  def on_any_event(self, event):
    logger.info("Got event {event}".format(event=event))
    if not self._re.search(event.src_path):
      logger.debug(
          "Igoring file={file}, regex miss!".format(file=event.src_path))
      return
    return super().on_any_event(event)

  def on_closed(self, event: FileClosedEvent):
    """ When a file is closed, upload it to s3 dst_url+relpath(filename)
    TODO(sami): use a threadpool down the road for uploads
    """
    if not event.is_directory and event.src_path in self._updated:
      self._uploader.enqueue_upload(event.src_path)
      self._updated.pop(event.src_path)
    return super().on_closed(event)

  def on_created(self, event):
    if not event.is_directory:
      self._updated[event.src_path] = True
    return super().on_created(event)

  def on_modified(self, event):
    if not event.is_directory:
      self._updated[event.src_path] = True
    return super().on_modified(event)

  # Should this apply a mv on uploader as well
  def on_moved(self, event):  # happens only when in monitored directory
    if not event.is_directory and event.src_path in self._updated:
      self._updated.pop(event.src_path, None)
      self._updated[event.dest_path] = True
    return super().on_moved(event)

  def on_deleted(self, event):
    if not event.is_directory:
      self._updated.pop(event.src_path, None)
    return super().on_deleted(event)
