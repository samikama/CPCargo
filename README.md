# CPCargo
A simple package to upload DL checkpoints to remote storage periodically.
See test for an example of how to use. In brief,
Construct a CheckpointCargo instance,
```
  cpc=CheckpointCargo(
               src_dir: str # local path where the checkpoints are saved, if folder, set recursive to True
               dst_url: str # s3 url for syncing checkpoints for example "s3://mybucket/my_project/experiment_x/checkpoints/"
               region: str, # s3 bucket region
               file_regex: str = ".*", # regex for selecting files in `src_dir`. Used to for constructing a regular expression. Only files matching the regex will be processed. Default, any file in the `src_dir`
               recursive: bool = False) -> None:
```
Then call `cpc.start()` to initialize watching the directory. I suggest calling after process already forks for data workers, i.e. after data pipeline is constructed.
Once the event loop ends, call `cpc.stop()` to finalize the process.
See `test/example.py` for an example of how it can be used, including signal handling for checkpoint on termination.
## Installation
Install from github with
`pip install https://github.com/samikama/CPCargo.git`

## Example use of `test/example.py`
below script will run(i.e. sleep a random duration) for 100 batches, creating a checkpoint(i.e. dummy files and directories) every 25 steps in `${HOME}/test/checkpoint_dir` directory and upload them to `s3://my_test_bucket/checkpoints/`. It also registers a signal handler such that if sigterm is sent, it will abort event loop and create a new checkpoint regardless of whether it is time or not, saving the most recent state. You can test this functionality by sending `kill -15 <pid>` from another shell. The process prints its self pid when it starts.
`python test/example.py -s ${HOME}/test/checkpoint_dir -r us-east-1 -b 100 -c 25 -d s3://my_test_bucket/checkpoints`
