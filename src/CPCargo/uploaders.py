import boto3, s3transfer
import os, logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("CPCargo")


class Uploader(ABC):

  def __init__(self) -> None:
    pass

  @abstractmethod
  def enqueue_upload(self, file):
    pass


class S3Uploader(Uploader):
  """ Uploads the modified files to S3 bucket
  TODO: error handling, threadpool use
  """
  _trfmgr: s3transfer.S3Transfer

  def __init__(self,
               region: str,
               src_prefix: str,
               dst_url: str,
               pool_size=0) -> None:
    self._pool_size = pool_size
    self._src_prefix = src_prefix
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
      self._trfmgr.upload_file(filename=file, bucket=self._bucket, key=dest_key)
    except Exception as e:
      logger.error(
          "Caught exception when trying to upload {f} to bucket={b} key={k} exception={x}"
          .format(f=file, b=self._bucket, k=dest_key, x=e))
