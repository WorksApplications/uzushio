import argparse as ap
from pathlib import Path
from collections import Counter
import itertools as it

import numpy as np
import suffixarray

doc_delim = "\n\n"


def main():
    args = parse_args()
    validate_args(args)

    data = ""
    for p in args.input:
        with p.open() as fin:
            data += fin.read()

    n_doc = len(data.split(doc_delim))
    min_len = args.min_len
    min_freq = n_doc * args.min_freq if args.min_freq < 1 else args.min_freq

    # use results for the reversed text to make result set closed
    # (do not want to handle every prefix/suffix of substrings)
    sa = suffixarray.SuffixArray(data)
    sa_rev = suffixarray.SuffixArray(data[::-1])

    def key_func(s, i, l, c):
        # take if target substr has enough length and freq count
        # also check new-line to handle per sentence
        return l >= min_len and c >= min_freq and s[i] == '\n' and s[i+l-1] == '\n'

    ss_cnt = {sa.str[x:x+l]: c
              for x, l, c in sa.iter_repeated_substrings(key=key_func)}
    rev_cnt = {sa_rev.str[x+l-1:x-1:-1]: c
               for x, l, c in sa_rev.iter_repeated_substrings(key=key_func)}
    common_ss = set(ss_cnt.keys()) & set(rev_cnt.keys())

    with args.output.open("w") as fout:
        for ss in common_ss:
            fout.write(f"{ss_cnt[ss]}\n{ss}{doc_delim}")

    return


def parse_args():
    parser = ap.ArgumentParser()
    parser.add_argument(dest="input", type=str, nargs="+",
                        help="Input text file.")

    parser.add_argument("--min-len", type=int,
                        default=10, help="minimum length")
    parser.add_argument("--min-freq", default=10, help="minimum frequency")

    parser.add_argument("-o", "--output", dest="output", type=str, default="./ss.txt",
                        help="File to output summary.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite output files when they already exist.")

    args = parser.parse_args()
    args.input = [Path(s) for s in args.input]
    args.output = Path(args.output)
    args.min_freq = float(args.min_freq)
    return args


def validate_args(args):
    if not args.overwrite:
        if args.output.exists():
            raise ValueError(
                f"File {args.output} already exists. Set --overwrite to continue anyway.")
    return


if __name__ == "__main__":
    main()
