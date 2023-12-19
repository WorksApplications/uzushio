import argparse
from dataclasses import dataclass
from pathlib import Path
from multiprocessing.pool import ThreadPool
import csv


@dataclass
class Args(object):
    input: list[Path]
    output: Path
    workers: int

    @staticmethod
    def parse() -> "Args":
        p = argparse.ArgumentParser()
        p.add_argument("--output", type=Path, required=True)
        p.add_argument("--workers", default=4, type=int)
        p.add_argument("input", type=Path, nargs="+")
        return Args(**vars(p.parse_args()))


def directory_size(p: Path) -> int:
    # print(f"calcluating size of {p}")
    result = 0
    for p in p.iterdir():
        result += p.stat().st_size
    return result


def print_progress(data):
    try:
        from tqdm import tqdm
    except ImportError:
        return
    for v in tqdm(data.values()):
        v.wait()


class Processor(object):
    def __init__(self, args: Args) -> None:
        self.args = args
        self.executor = ThreadPool(args.workers)

    def run(self):
        matrix = {}

        for input_dir in self.args.input:
            for child in input_dir.iterdir():
                chname = child.name
                if not chname.startswith("segment="):
                    continue
                segment = chname[8:]
                res = self.process_segment(segment, child)
                matrix.update(res)

        self.executor.close()

        filters = set()
        segments = set()

        for segment, filter in matrix.keys():
            filters.add(filter)
            segments.add(segment)

        filters = sorted(filters)
        segments = sorted(segments)

        print_progress(matrix)

        self.executor.join()

        with self.args.output.open("wt", newline="\n") as of:
            wr = csv.writer(of)

            wr.writerow([""] + filters)

            for segment in segments:
                row = [segment]
                for filter in filters:
                    v = matrix.get((segment, filter), None)
                    if v is None:
                        r = ""
                    else:
                        r = str(v.get())
                    row.append(r)
                wr.writerow(row)

    def process_segment(self, segment: str, segment_dir: Path):
        result = {}
        for child in segment_dir.iterdir():
            chname = child.name
            if not chname.startswith("filter="):
                continue

            filter = chname[7:]
            result[(segment, filter)] = self.executor.apply_async(
                directory_size, [child]
            )

        return result


if __name__ == "__main__":
    args = Args.parse()
    p = Processor(args)
    p.run()
