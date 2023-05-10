from multiprocessing import Queue, Process
import logging, time, os, signal, typing
from queue import Empty


def get_logger():
  logger = logging.getLogger("Heartbeat")
  ch = logging.StreamHandler()
  formatter = logging.Formatter(
      fmt=
      '%(asctime)s.%(msecs)03d %(name)s %(filename)s:%(funcName)s:%(lineno)d %(message)s',
      datefmt='%Y-%m-%d,%H:%M:%S')
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  return logger


logger = get_logger()
check_time = None
pulse = None


def signal_handler(signum, frame):
  global check_time
  global pulse
  logger.warn("Received {sig}, check_time={ct} pulse={p} frame={fr}".format(
      sig=signal.Signals(signum).name, ct=check_time, p=pulse, fr=frame))


def hang_watcher(parent_pid: int, default_timeout: int, queue: Queue,
                 kill_timeout: int):
  signal.signal(signal.SIGUSR1, signal_handler)
  global check_time
  global pulse
  check_time = None
  pulse = False
  logger.info("Starting parent watcher for pid={pid} mypid={mpid}".format(
      pid=parent_pid, mpid=os.getpid()))
  while True:
    curr = time.perf_counter()
    if check_time and curr > check_time:
      logger.warn(
          "Timeout reached. Expected ping at {target} but it is {now}".format(
              target=check_time, now={curr}))
      os.kill(parent_pid, signal.SIGTERM)
      time.sleep(kill_timeout)
      os.kill(parent_pid, signal.SIGKILL)
      return
    # we need to drain the queue if steps are faster than sleep time
    try:
      # drain the queue until it is empty
      first_pass = True
      while True:
        task, duration = queue.get_nowait()
        if task == "TERMINATE":
          return
        elif task == "START":
          if check_time and not pulse and first_pass:
            logger.warn("Start called without stop!")
          if not duration:
            duration = default_timeout
          check_time = curr + duration
          pulse = False
        elif task == "STOP":
          if not check_time and not pulse and first_pass:
            logger.warn("Got stop before start!")
          pulse = False
          check_time = None
        elif task == "PULSE":
          if check_time and not pulse and first_pass:
            logger.warn("Pulse is called before last timer stopped!")
          pulse = True
          check_time = curr + default_timeout
        first_pass = False
    except Empty:
      pass
    except Exception as e:
      logger.error("Unexpected error happened. {err}".format(err=e))
    # wait for 1 sec
    time.sleep(1)


class Heartbeat():
  _queue: Queue
  _process: Process
  _timeout: int
  _kill_timeout: int

  def __init__(self, timeout: int, kill_timeout: int = 15) -> None:
    """
    timeout     : default timeout for pulse() function, integer, minimum 1
    kill_timeout: time difference between initial SIGTERM and final SIGKILL, int, default 15.
                   Set it to 0 for immediate termination.
    """
    self._queue = Queue()
    self._timeout = timeout
    self._kill_timeout = kill_timeout
    ppid = os.getpid()
    self._process = Process(
        target=hang_watcher,
        args=[ppid, self._timeout, self._queue, self._kill_timeout])
    self._process.start()
    logger.warn(
        "Started heartbeat checking process with default timeout {timeout}, process pid={pp}"
        .format(timeout=timeout, pp=self._process.pid))

  def __del__(self):
    logger.warn("Terminating subprocess")
    self._queue.put_nowait(("TERMINATE", None))
    self._process.join()

  def pulse(self):
    """ Start or reset the kill timer to default timeout
    """
    self._queue.put_nowait(("PULSE", self._timeout))

  def start(self, timeout: typing.Optional[int] = None):
    """ Starts the kill timer with timeout or default timeout
    """
    self._queue.put_nowait(("START", timeout))

  def stop(self):
    """ Stops the current kill timer
    """
    self._queue.put_nowait(("STOP", None))
