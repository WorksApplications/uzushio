import os
import re
import gc
from glob import glob
from argparse import ArgumentParser

import pandas as pd


def cal_overlap(args):
    if not args.dump_direc_path.endswith("/"):
        args.dump_direc_path += "/"
    dump_paths = glob(args.dump_direc_path + "*/")

    if os.path.isfile(args.output_path):
        mode = "a"
        df_old = pd.read_csv(args.output_path)
    else:
        mode = "w"
    
    with open(args.output_path, mode) as f:
        if mode == "w":
            f.write(f"dump_1,dump_2,len_dump_1,len_dump_2,overlap_ratio1,overlap_ratio2,overlap_ratio3\n")

        for i in range(len(dump_paths)):
            dump_1 = re.search(r'\d{4}-\d{2}', dump_paths[i])[0]
            df = pd.read_parquet(dump_paths[i], columns=["url"])
            url_set_dump_1 = set(df["url"].tolist())

            for j in range(i + 1, len(dump_paths)):
                dump_2 = re.search(r'\d{4}-\d{2}', dump_paths[j])[0]
                
                # skip if (dump_1, dump_2) was already calculated
                if mode == "a" and (len(df_old[(df_old["dump_1"] == dump_1) & (df_old["dump_2"] == dump_2)]) != 0 or \
                                    len(df_old[(df_old["dump_1"] == dump_2) & (df_old["dump_2"] == dump_1)]) != 0):
                    continue
                    
                df = pd.read_parquet(dump_paths[j], columns=["url"])
                url_set_dump_2 = set(df["url"].tolist())
                
                overlap = url_set_dump_1 & url_set_dump_2
                universe = url_set_dump_1 | url_set_dump_2
                overlap_ratio1 = len(overlap) / len(url_set_dump_1) * 100.0
                overlap_ratio2 = len(overlap) / len(url_set_dump_2) * 100.0
                overlap_ratio3 = len(overlap) / len(universe) * 100.0
                
                f.write(f"{dump_1},"\
                        f"{dump_2},"\
                        f"{len(url_set_dump_1)},"\
                        f"{len(url_set_dump_2)},"\
                        f"{overlap_ratio1:.2f},"\
                        f"{overlap_ratio2:.2f},"\
                        f"{overlap_ratio3:.2f}\n")
                
                del df, url_set_dump_2
                gc.collect()
            del url_set_dump_1
            gc.collect()
    
    
def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--dump_direc_path",
        type=str,
        help="Path to the extracted common crawl dumps.",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="Path for the output csv file.",
    )
    args = parser.parse_args()
    cal_overlap(args)
    

if __name__ == "__main__":
    main()