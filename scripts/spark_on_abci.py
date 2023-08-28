import argparse
import dataclasses
import os
import socket
import subprocess
import time
from pathlib import Path


@dataclasses.dataclass
class Args:
    jar: str
    clazz: str
    spark: Path
    logroot: Path
    options: list[str]
    local_dir: str = None
    ping: Path = None

    @staticmethod
    def parse() -> 'Args':
        p = argparse.ArgumentParser()
        p.add_argument('--jar', required=True)
        p.add_argument('--class', required=True)
        p.add_argument('--spark', required=True, type=Path)
        p.add_argument('--ping', type=Path)
        p.add_argument('--logroot', type=Path, default=_default_scratch())
        args, opts = p.parse_known_args()
        return Args(options=opts, **vars(args))


def _env_local_dir() -> str:
    return os.getenv("SGE_LOCALDIR")


def _current_hosts() -> list[str]:
    hosts_file = os.getenv("PE_HOSTFILE")
    hosts = []
    for line in open(hosts_file, 'rt'):
        space_idx = line.find(' ')
        if space_idx != -1:
            hosts.append(line[:space_idx])
    hosts.sort()
    return hosts


def _job_id() -> str:
    return os.getenv("JOB_ID") or "nojob"


def _default_scratch() -> Path:
    user = os.getenv("USER") or "nouser"
    return Path("/scratch") / user / "spark" / _job_id()


def _spark_dfs_logdir() -> Path:
    user = os.getenv("USER") or "nouser"
    return Path("/scratch") / user / "spark-logs"


def _hostname_opts() -> set[str]:
    hostname = socket.gethostname()
    parts = hostname.split('.')
    result = set()
    for i in range(1, len(parts) + 1):
        result.add('.'.join(parts[0:i]))
    return result


class SparkLauncher(object):
    def __init__(self, args: Args):
        self.args = args
        self.local_dir = args.local_dir or _env_local_dir()
        self.hosts = _current_hosts()
        self.head = self.hosts[0]
        self.hostnames = _hostname_opts()

    def is_master(self):
        return self.head in self.hostnames

    def ping(self, data):
        if self.args.ping is None:
            return
        with open(self.args.ping, 'at') as f:
            f.write(data)
            f.write('\n')

    def launch_master(self):
        self.ping(f"launched master spark://{self.head}:7077, web ui http://{self.head}:8080\t-L 8080:{self.head}:8080")
        return subprocess.Popen(
            executable="bash",
            args=[
                str(self.args.spark / "sbin/start-master.sh")
            ],
            env={
                'SPARK_LOG_DIR': self.args.logroot,
                'SPARK_NO_DAEMONIZE': 'true'
            }
        )

    def worker(self):
        return subprocess.Popen(
            executable="bash",
            args=[
                str(self.args.spark / "sbin/start-executor.sh"),
                f"spark://{self.head}:7077"
            ],
            env={
                'SPARK_LOCAL_DIRS': self.local_dir,
                'SPARK_LOG_DIR': self.args.logroot,
                'SPARK_NO_DAEMONIZE': 'true'
            }
        )

    def launch_driver(self):
        proc = subprocess.Popen(
            executable='bash',
            args=[
                     self.args.spark / "bin/spark-submit.sh",
                     "--class", self.args.clazz,
                     "--master", f"spark://{self.head}:7077",
                     "--conf", "spark.driver.memory=30G",
                     "--conf", "spark.executor.memory=63G",
                     "--conf", "spark.executor.extraJavaOptions=-XX:ObjectAlignmentInBytes=16",
                     "--conf", "spark.driver.log.persistToDfs.enabled=true",
                     "--conf", f"spark.driver.log.dfsDir={_spark_dfs_logdir()}",
                     "--conf", f"spark.eventLog.dir={_spark_dfs_logdir()}",
                     "--conf", "spark.eventLog.enabled=true",
                     "--conf", "spark.eventLog.compress=true",
                     "--conf", "spark.checkpoint.compress=true",
                     "--conf", "spark.memory.offHeap.enabled=true",
                     "--conf", "spark.memory.offHeap.size=100G",
                     self.args.jar
                 ] + self.args.options
        )
        self.ping(f"launched executor http://{self.head}:4040\t-L 4040:{self.head}:4040")
        proc.wait()

    def launch(self):
        if self.is_master():
            master = self.launch_master()
        worker = self.worker()
        if self.is_master():
            time.sleep(5)  # sleep 5 secs
            self.launch_driver()


def main(args: Args):
    pass


if __name__ == '__main__':
    main(Args.parse())
