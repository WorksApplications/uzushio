# About Uzushio

Uzushio is a web-scale corpus text extraction and preprocessing tool.
It uses Spark and written mostly in Scala.

## Goal of this project

LLMs and foundational models require huge clean corpus for training.
There are many cleaning methods, and each nlp tasks have suitable sets of them.

Uzushio has following goals:

- provide a common corpus cleaning tool
  - reduce the implementation cost
  - share same implemantation for the compatibility
- keep it efficient, flexible and updated
  - it should be able to used in every kind of nlp projects (which uses large corpus)

## References

- [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn)
  - preprocess in chitra pretraining
- [Deduplicating Training Data Makes Language Models Better](https://arxiv.org/abs/2107.06499)

# Setup

You need to install Apache Spark, sbt, and Sudachi dictionary file.

## Apache Spark (if running locally, if you want to use AWS EMR you do not need it)

Download it from the [official page](https://spark.apache.org/downloads.html). 
You can put it in PATH or specify absolute paths to 

At the moment uzushio is built against: `spark 3.4.*` + `Scala 2.12.*`.

Check if `spark-submit --version` works.

## Scala (sbt)

Download sbt (latest 1.* version) from the [official page](https://www.scala-sbt.org/download.html) and make it usable.
You can often install one from OS package manager.

### JDK

You need JDK 11+ to run sbt and Spark.

## WIP: Sudachi (Outdated, will download dictionary automatically in future if needed)

Some features uses sudachi. When you want to use those, you need a sudachi dictionary.

[download](http://sudachi.s3-website-ap-northeast-1.amazonaws.com/sudachidict/) sudachi dictionary file (.dict) and place in the root dir.

todo: specify dict by config/args

# WIP: Running

## Build fat JAR for launching

The easiest way to use the tool — build fat JAR with all required dependencies.

Execute `sbt assembly` from project root directory.
You will have output jar under `./target/scala-[version]/`.

### note

- 開発段階などでコンパイルを繰り返すなら `sbt` で sbt-shell を起動しておいた方が速い
- `sbt compile` ではなく `sbt assembly`を使う
  - spark に投げるために必要ライブラリ等をまとめてバンドルするため
    - なお spark は除外する必要があり、`build.sbt`で設定している
  - `./project/plugins.sbt` にて設定している

## submit

`spark-submit` に jar とオプションを投げる：

```
spark-submit [spark options] --class [class name] [path/to/jar_file] [class options]
```

各処理のオプションについては下記、
spark 側の詳細は [`spark-submit` のヘルプ](https://spark.apache.org/docs/latest/submitting-applications.html)を参照。

# 実行クラス

## CorpusCleaner

入力コーパスを整形する。

処理の流れについてはコンフィグファイルで指定する。

```
spark-submit --class org.sample.corpus.CorpusCleaner \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=../data/*.txt --output=./out \
    --config "chitra"
```

### args

- `--input`: Input corpus. List of path to the file or dir (load all files in the dir).
  - Multiple input is allowed (ex. `--input ./file.a ./input_dir/ ./and_more/*.txt`).
  - By default, each files are treated as "\n\n" splitted documents that consists of "\n" splitted sentences.
- `--output`: Spark output dir (default `./out`). Need to be empty if exists.
- `--config`: Name or path of config file (default: `chitra`).
  - See `src/main/resources/reference.conf` for reference.
  - See next section for existing config name.
- You can override config values by cli arg (check with `-h` option).

### 既存のコンフィグ

既存の処理についてはそれぞれコンフィグファイルとしてまとめてある。
これらについては `--config` オプションにて名称での指定が可能。

- `chitra`
  - [sudachitra での整形](https://github.com/WorksApplications/SudachiTra/tree/main/pretraining/bert#2-preprocessing-corpus-cleaning) と同等の処理を行う。
  - ref: [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn)
- `rmTemplate`
  - minhash 適用の調査時に chitra 前処理後の追加処理として作成したもの。
    - 重複文書（完全一致）
    - 指定したテンプレート文（or 段落）
    - 連続して繰り返される同一文（１文のみ残す）
- `sudachiDictCorpus`
  - sudachi 辞書検証用コーパスのクリーニング用コンフィグ。
- `warc`
  - `warc.WarcToDocument` で抽出したテキストのクリーニング用コンフィグ。
  - 日本語テキストの選別、段落単位での重複除去の後、`chitra` + `rmTemplate` の処理を行う。
  - こののち `MinHashDeduplicator` を適用する想定。

## MinHashDeduplicator

類似文書を削除する。

文書を n-gram の集合とみなし、Jaccard distance に基づき類似文書をリストアップ、削除する。

[Deduplicating Training Data Makes Language Models Better](https://arxiv.org/abs/2107.06499) における NearDup を再現したもの。

```
spark-submit --class org.sample.corpus.MinHashDeduplicator \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=./data/nwjc/* --output=./out \
    --mode C \
    --ngram 5 \
    --minhash-b 5 \
    --minhash-r 10 \
    --jaccard-thr 0.8 \
    --skip-editsim
```

### args

- `--input`, `--output`: Input files and output directory. Same as CorpusCleaner.
  - Deduplicated documents are written into `${output}/dedup`
  - Removed documents are written into `${output}/dup`
- `--mode`: Sudachi split mode (A/B/C). (default C)
- `--ngram`: n of n-gram, used to convert document to a set of n-grams. (default 5)
- `--minhash-b`: And-amplification value for minhash-lsh. (default 15)
- `--minhash-r`: Or-amplification value for minhash-lsh. (default 150)
- `--jaccard-thr`: Threshold for jaccard index. Document pairs with JI less than this will be skipped. (default 0.8)
- `--editsim-thr`: Threshold for edit similarity. Document pairs with ES less than this will be skipped. (default 0.8)
- `--skip-editsim`: Set to skip the filtering by the edit similarity.

#### MinHashLSH

The minhash-lsh lists the pairs of similar documents in a probabilistic way, considering a document as a set of n-gram.

Let `s` be a [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index) of a document pair.
Then the probability of the pair is selected equals to `1 - (1 - x^b)^r`.

You should change hyper parameter `b` and `r` according to the threshold you want.
The default parameter is taken from the reference paper that uses JD 0.8 as a threshold
([ref: graph of the matching probability](https://www.desmos.com/calculator/jq7mpurg3m)).

The computation cost will increases linear to `b*r`, which is the number of hashes used in minhash alg.
Also note that the storage used by minhash increases linear to `r`.

#### Filtering by actual similarity

After the MinHashLSH, we filter out document pairs with low similarity.
This is neccessarly since MinHash is a probabilistic algorithm.

We use following two metrics:

- Jaccard index
- Edit similarity
  - def: EditDistance(d1, d2) / max(d1.length, d2.length)
    - ref: [edit distance](https://en.wikipedia.org/wiki/Edit_distance)
  - This takes `O(T^2)` time with a document length `T`, will is so long.
    - Skip this step with `--skip-editsim` option.

## warc.WarcToDocument

Extract text and meta data from WARC file.

See [[./src/main/scala/org/sample/corpus/warc/README.md]] for detail.

```bash
spark-submit --class org.sample.corpus.warc.WarcToDocument \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=./data/nwjc/01warc/ --output=./out \

# this will also create `out_fulldata` dir next to the output dir.
```

# references

## WARC

- [warc format specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0)
  - Note that latest version is v1.1 but CC uses v1.0
- [org.archive.io](http://crawler.archive.org/apidocs/org/archive/io/package-summary.html)
  - Iterates over warc records
- [com.martinkl.warc](https://github.com/ept/warc-hadoop)
  - Loads warc file using Hadoop API
