import sys
import json
import gzip
from tqdm import tqdm
from transformers import AutoTokenizer
import os

tokenizer = AutoTokenizer.from_pretrained("llm-jp/llm-jp-13b-v1.0")


def count_tokens(input_file):
    num_tokens = 0
    compressed_size = os.path.getsize(input_file)

    with gzip.open(input_file, "rb") as f:
        for line in tqdm(f):
            example = json.loads(line)
            text = example["text"]
            tokens = tokenizer.encode(text)
            num_tokens += len(tokens)

    tokens_per_byte = num_tokens / compressed_size
    return num_tokens, tokens_per_byte


if __name__ == "__main__":
    input_file = sys.argv[1]
    num_tokens, tokens_per_byte = count_tokens(input_file)
    print(f"Total number of tokens: {num_tokens}")
    print(f"Tokens per byte: {tokens_per_byte:.3f}")
