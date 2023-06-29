import boto3, s3transfer
import os, logging, threading, time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("CPCargo")


class Uploader(ABC):

  def __init__(self) -> None:
    pass

  @abstractmethod
  def enqueue_upload(self, file):
    pass


# From S3Transfer object doc
class CompletionCallback(object):

  def __init__(self, filename, waiter):
    self._filename = filename
    self._size = os.path.getsize(filename)
    self._seen_so_far = 0
    self._waiter = waiter
    self._lock = threading.Lock()

  def __call__(self, bytes_amount):
    # To simplify we'll assume this is hooked up
    # to a single filename.
    with self._lock:
      self._seen_so_far += bytes_amount
      if self._seen_so_far >= self._size:
        self._waiter.done(self._filename)


class CompletionWaiter:

  def __init__(self, timeout=30) -> None:
    self._timeout = timeout
    self.active_files = set()
    self._lock = threading.Lock()

  def get_callback(self, filename):
    with self._lock:
      self.active_files.add(filename)
      return CompletionCallback(filename=filename, waiter=self)

  def done(self, filename):
    with self._lock:
      try:
        self.active_files.remove(filename)
      except KeyError as e:
        logger.error(
            "Got file completed for {f} but wasn't tracking that upload!".
            format(f=filename))

  def wait(self, timeout=None):
    now = time.perf_counter()
    tend = now + timeout if timeout else now + self._timeout
    while time.perf_counter() < tend:
      with self._lock:
        if not len(self.active_files):
          break
      time.sleep(1)
    logger.info("Outstanding list of transfers {t}".format(t=self.active_files))


class S3Uploader(Uploader):
  """ Uploads the modified files to S3 bucket
  TODO: error handling, threadpool use
  """
  _trfmgr: s3transfer.S3Transfer

  def __init__(self,
               region: str,
               src_prefix: str,
               dst_url: str,
               waiter: CompletionWaiter,
               pool_size=0) -> None:
    self._pool_size = pool_size
    self._src_prefix = src_prefix
    self._waiter = waiter
    if not dst_url.lower().startswith("s3://"):
      raise ValueError("destination url should start with 's3://'")
    dst_paths = os.path.split(dst_url[5:])
    if not dst_paths:
      logger.error("Malformed s3 upload url! {url}".format(url=dst_url))
      raise ValueError("Malformed s3 upload url! {url}".format(url=dst_url))
    self._bucket = dst_paths[0]
    if len(dst_paths) == 1:
      logger.warning("Warning upload directory is set as root of the bucket!")
      self._key_prefix = ""
    else:
      self._key_prefix = os.path.join(*dst_paths[1:])
    self._dst = dst_url
    try:
      self._client = boto3.client("s3", region_name=region)
      self._trfmgr = s3transfer.S3Transfer(client=self._client)
    except Exception as e:
      logger.error("Failed to create s3 client. error={error}".format(error=e))
      raise e

  def find_key(self, file_path):
    rel_path = os.path.relpath(file_path, self._src_prefix)
    if not rel_path:  #is srcpath a file?
      rel_path = os.path.split(file_path)[-1]
    s3_path = os.path.join(self._key_prefix, rel_path)
    logger.info("File {local} changed. Uploading to remote {remote}".format(
        local=rel_path, remote=s3_path))
    return s3_path

  def enqueue_upload(self, file):
    dest_key = self.find_key(file)
    try:
      self._trfmgr.upload_file(
          filename=file,
          bucket=self._bucket,
          key=dest_key,
          callback=self._waiter.get_callback(filename=file))

    except Exception as e:
      logger.error(
          "Caught exception when trying to upload {f} to bucket={b} key={k} exception={x}"
          .format(f=file, b=self._bucket, k=dest_key, x=e))
