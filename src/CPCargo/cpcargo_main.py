# main function for mostly testing
from argparse import ArgumentParser as AP
import sys, time
from CPCargo import CheckpointCargo


def parse_arguments():
  parser = AP("Checkpoint Cargo")
  parser.add_argument('-r', '--region', default='us-west-2')
  parser.add_argument('-p', '--profile', default=None)
  parser.add_argument('-s', '--source-dir', default=None, required=True)
  parser.add_argument('-d', '--destination-url', default=None, required=True)
  parser.add_argument('-f', '--file-pattern', default=".*")
  parser.add_argument('-R', '--recursive', action='store_true')
  args, unknown = parser.parse_known_args()
  return args, unknown


def main():
  args, unknown = parse_arguments()
  cargo = CheckpointCargo(src_dir=args.source_dir,
                          dst_url=args.destination_url,
                          region=args.region,
                          file_regex=args.file_pattern,
                          recursive=args.recursive)
  cargo.start()
  time.sleep(100)
  cargo.stop()
  return 0


if __name__ == "__main__":
  sys.exit(main())
