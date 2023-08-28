import argparse
import dataclasses
import datetime
import os
import socket
import subprocess
import time
from pathlib import Path
import psutil


@dataclasses.dataclass
class Args:
    jar: str
    clazz: str
    spark: Path
    logroot: Path
    options: list[str]
    local_dir: str = None
    ping: Path = None
    driver_mem: str = "30G"
    executor_mem: str = "63G"
    offheap_mem: str = "100G"

    @staticmethod
    def parse() -> 'Args':
        p = argparse.ArgumentParser()
        p.add_argument('--jar', required=True)
        p.add_argument('--class', required=True, dest='clazz')
        p.add_argument('--spark', required=True, type=Path)
        p.add_argument('--ping', type=Path)
        p.add_argument('--logroot', type=Path, default=_default_scratch())
        p.add_argument('--driver-mem', default='30G')
        p.add_argument('--executor-mem', default='63G')
        p.add_argument('--offheap-mem', default='100G')
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

def killall(root: subprocess.Popen):
    proc = psutil.Process(root.pid)
    children = proc.children(recursive=True)
    children.reverse()
    for child in children:
        child.terminate()
    for child in children:
        child.wait()



class SparkLauncher(object):
    def __init__(self, args: Args):
        self.args = args
        self.local_dir = args.local_dir or _env_local_dir()
        self.hosts = _current_hosts()
        self.head = self.hosts[0]
        self.hostnames = _hostname_opts()

    def print_debug_conf(self):
        print(self.args)
        print(self.local_dir)
        print(self.hosts)
        print(self.head, self.is_master())
        print(self.hostnames)
        print(_default_scratch(), _spark_dfs_logdir())

    def is_master(self):
        return self.head in self.hostnames

    def ping(self, data):
        ping_path = self.args.ping
        if ping_path is None:
            return

        ping_parent = ping_path.parent
        if not ping_parent.exists():
            ping_parent.parent.mkdir(parents=True)

        now = datetime.datetime.now()

        with open(ping_path, 'at') as f:
            f.write(now.isoformat())
            f.write('\t')
            f.write(_job_id())
            f.write('\t')
            f.write(data)
            f.write('\n')

    def launch_master(self):
        self.ping(f"launched master spark://{self.head}:7077, web ui http://{self.head}:8080\t-L 8080:{self.head}:8080")
        env = dict(os.environb)
        env['SPARK_LOG_DIR'] = self.args.logroot
        env['SPARK_NO_DAEMONIZE'] = 'true'
        return subprocess.Popen(
            args=[
                "/bin/bash",
                str(self.args.spark / "sbin/start-master.sh")
            ],
            env=env
        )

    def make_dirs(self):
        Path(_default_scratch()).mkdir(parents=True, exist_ok=True)
        Path(_spark_dfs_logdir()).mkdir(parents=True, exist_ok=True)
        self.args.logroot.mkdir(parents=True, exist_ok=True)

    def launch_worker(self):
        env = dict(os.environb)
        env['SPARK_LOCAL_DIRS'] = self.local_dir
        env['SPARK_LOG_DIR'] = self.args.logroot
        env['SPARK_NO_DAEMONIZE'] = 'true'

        return subprocess.Popen(
            args=[
                "/bin/bash",
                str(self.args.spark / "sbin/start-worker.sh"),
                f"spark://{self.head}:7077"
            ],
            env=env
        )

    def launch_driver(self):
        proc = subprocess.Popen(
            args=[
                "/bin/bash",
                     str(self.args.spark / "bin/spark-submit"),
                     "--class", self.args.clazz,
                     "--master", f"spark://{self.head}:7077",
                     "--conf", f"spark.driver.memory={self.args.driver_mem}",
                     "--conf", f"spark.executor.memory={self.args.executor_mem}",
                     "--conf", "spark.executor.extraJavaOptions=-XX:ObjectAlignmentInBytes=16",
                     "--conf", "spark.driver.log.persistToDfs.enabled=true",
                     "--conf", f"spark.driver.log.dfsDir={_spark_dfs_logdir()}",
                     "--conf", f"spark.eventLog.dir={_spark_dfs_logdir()}",
                     "--conf", "spark.eventLog.enabled=true",
                     "--conf", "spark.eventLog.compress=true",
                     "--conf", "spark.checkpoint.compress=true",
                     "--conf", "spark.memory.offHeap.enabled=true",
                     "--conf", f"spark.memory.offHeap.size={self.args.offheap_mem}",
                     self.args.jar
                 ] + self.args.options
        )
        self.ping(f"launched driver http://{self.head}:4040\t-L 4040:{self.head}:4040")
        return proc

    def launch(self):
        self.make_dirs()
        if self.is_master():
            master = self.launch_master()
            worker = self.launch_worker()
            time.sleep(5)  # sleep 5 secs
            driver = self.launch_driver()
            driver.wait()
            killall(worker)
            killall(master)
            master.wait()
            worker.wait()
        else:
            worker = self.launch_worker()
            worker.wait()



def main(args: Args):
    launcher = SparkLauncher(args)
    launcher.launch()


if __name__ == '__main__':
    main(Args.parse())
