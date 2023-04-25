import os, logging, signal, time
import random
from argparse import ArgumentParser as AP
from CPCargo import CheckpointCargo

SignalCheckpoint = False
logging.basicConfig(
    format=
    '%(asctime)s.%(msecs)03d %(name)s %(filename)s:%(funcName)s:%(lineno)d %(message)s',
    datefmt='%Y-%m-%d,%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger("CPCargoExample")


def checkpoint(checkpoint_id, checkpoint_path):
  """ Mimic a checkpoint creation
  """
  if not os.path.exists(checkpoint_path):
    os.mkdir(checkpoint_path)
  if not os.path.isdir(checkpoint_path):
    raise RuntimeError("Checkpoint path is a file!")
  curr_dir = os.path.join(checkpoint_path,
                          "tensors_steps{id}".format(id=checkpoint_id))
  if not os.path.exists(curr_dir):
    os.mkdir(curr_dir)
  for i in range(3):
    with open(os.path.join(curr_dir, f"tensor_{i}"), 'w') as f:
      f.write("This is tensor {i}".format(i=i))


def signal_handler(signum, frame):
  # trigger a checkpoint and then exit cleanly
  # do kill -15 <pid> from a separate shell to trigger checkpoint-then-exit behavior
  logger.info("Got signal {sig}".format(sig=signal.Signals(signum).name))
  if signum == signal.SIGTERM:
    global SignalCheckpoint
    SignalCheckpoint = True


def parse_arguments():
  parser = AP("Checkpoint cargo example script")
  parser.add_argument('-r', '--region', default='us-west-2')
  parser.add_argument('-p', '--profile', default=None)
  parser.add_argument('-s', '--source-dir', default=None, required=True)
  parser.add_argument('-d', '--destination-url', default=None, required=True)
  parser.add_argument('-b', '--num-batches', default=100, type=int)
  parser.add_argument('-c', '--checkpoint_period', default=25, type=int)
  args, unknown = parser.parse_known_args()
  return args, unknown


class DataPipeline():

  def __init__(self, num_entries) -> None:
    self._nmax = num_entries
    self._n = 0

  def __iter__(self):
    return self

  def __next__(self):
    return self.next()

  def next(self):
    if self._n < self._nmax:
      self._n += 1
      return random.randint(2, 6)
    raise StopIteration()


class Model:

  def __init__(self) -> None:
    self._step = 0

  def step(self, batch):
    logger.info("Processing(zzz (-_-) ) step {step}".format(step=self._step))
    self._step += 1
    time.sleep(batch)
    return batch


class Executor():
  """ An example training loop manager
  """
  model: Model

  def __init__(self, data_pipeline, model, config) -> None:
    self.data_pipeline = data_pipeline
    self.model = model
    self.config = config

  def train(self):
    global SignalCheckpoint
    c = self.config
    # initialize CheckpointCargo
    CP = CheckpointCargo(src_dir=c["checkpoint_path"],
                         dst_url=c["s3_path"],
                         region=c["region"],
                         file_regex=r'.*',
                         recursive=True)
    # You can start monitoring checkpoint directory after data pipeline subprocesses forks
    CP.start()
    for id, batch in enumerate(self.data_pipeline):
      self.model.step(batch)
      if SignalCheckpoint:
        checkpoint(id, c["checkpoint_path"])
        break
      if id % c["cp_period"] == 0:
        checkpoint(id, c["checkpoint_path"])
    CP.stop()


def get_datapipeline(n_entries):
  return DataPipeline(n_entries)


def main():
  logger.info(f"Starting example at PID= {os.getpid()}")
  # set a signal handler for sigterm to capture termination request!
  signal.signal(signal.SIGTERM, signal_handler)
  args, unknown = parse_arguments()
  data_pipeline = get_datapipeline(args.num_batches)

  config = dict()
  config["checkpoint_path"] = args.source_dir
  config["s3_path"] = args.destination_url
  config["region"] = args.region
  config["cp_period"] = args.checkpoint_period
  e = Executor(data_pipeline=data_pipeline, model=Model(), config=config)
  e.train()
  logger.info("Training finished")


if "__main__" in __name__:
  main()
