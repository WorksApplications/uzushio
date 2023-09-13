import argparse
import dataclasses
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow.csv as pcsv


@dataclasses.dataclass
class Args(object):
    output: str
    input: list[str]
    title: str = None

    @staticmethod
    def parse():
        p = argparse.ArgumentParser()
        p.add_argument("--output", type=Path)
        p.add_argument("--title")
        p.add_argument("input", type=Path, nargs="+")
        return Args(**vars(p.parse_args()))


def plot_histogram(args: Args, folder_paths: list[Path]):
    histogram_data = []
    titles = []

    # Iterate through subfolders and CSV files
    for folder in folder_paths:
        if folder.is_dir():
            total_df = []
            csv_files = folder.glob("*.csv")
            for csv_file in csv_files:
                data = pcsv.read_csv(
                    csv_file,
                    read_options=pcsv.ReadOptions(column_names=["val", "text"]),
                    convert_options=pcsv.ConvertOptions(include_columns=["val"]),
                )

                total_df.append(data.column(0).to_numpy())

            total_df = np.concatenate(total_df, axis=0)
            histogram_data.append(total_df)
            titles.append(folder.name)

    plt.hist(
        histogram_data,
        bins=200,
        density=True,
        label=titles,
        histtype="stepfilled",
        alpha=0.5,
    )
    plt.legend(titles)
    plt.ylabel("Data %")
    plt.xlabel("Value")
    plt.title(args.title)


def main(args: Args):
    plot_histogram(args, args.input)
    plt.savefig(args.output)


if __name__ == "__main__":
    main(Args.parse())
