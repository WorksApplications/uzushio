# Corpus cleaning

大規模日本語コーパス (e.g. Wikipedia, NWJC, etc.) の整形のためのツール

Apach Spark (scala) ベース

## Goal of this project

It is known that ML model training with clean corpus is important.
There are many many cleaning methods, and each nlp tasks have suitable sets of them.

In this repository, we try to:

- provide a common corpus cleaning tool
  - reduce the implementation cost
  - share same implemantation for the compatibility
- keep it efficient, flexible and updated
  - it should be able to used in every kind of nlp projects (which uses large corpus)

## ref

- [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn)
  - preprocess in chitra pretraining
- [Deduplicating Training Data Makes Language Models Better](https://arxiv.org/abs/2107.06499)

# setup

You need to install apach-spark, scala (sbt), and sudachi dictionary file.

## apach spark

[download](https://spark.apache.org/downloads.html) and set path.

The version should be: `spark 3.2.1` + `Scala 2.12.15`.

check if `spark-submit --version` works.

## scala (sbt)

[download](https://www.scala-sbt.org/download.html) and set path.

Note that you should use compatible version to the one used by spark.

### Java

You may also need to setup Java environment.

## sudachi

[download](http://sudachi.s3-website-ap-northeast-1.amazonaws.com/sudachidict/) sudachi dictionary file (.dict) and place in the root dir.

todo: specify dict by config/args

# run

## build

root (`build.sbt`のあるディレクトリ) にて `sbt assembly` でコンパイルする

`./target/scala-[version]/` 以下に jar が生成される

### note

- 開発段階などでコンパイルを繰り返すなら `sbt` で sbt-shell を起動しておいた方が速い
- `sbt compile` ではなく `sbt assembly`を使う
  - spark に投げる用に必要ライブラリ等もまとめてバンドルするため
    - なお spark は除外する必要があり、`build.sbt`で設定している
  - `./project/plugins.sbt` にて設定している

## submit

`spark-submit` に jar とオプションを投げる：

```
spark-submit --class [class name] [path/to/jar_file] [args for cli]
```

ex.

```
spark-submit --class org.sample.corpus.CorpusCleaner \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=../data/*.txt --output=./out \
    --ngwords ./resources/ng_words.txt
```

spark 側の設定等は [`spark-submit` のヘルプ](https://spark.apache.org/docs/latest/submitting-applications.html)を参照

# 実行クラス

## CorpusCleaner

入力コーパスを整形する

現状は [sudachitra での整形](https://github.com/WorksApplications/SudachiTra/tree/main/pretraining/bert#2-preprocessing-corpus-cleaning) と同等（のはず）

ref: [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn)

現在フィルタ/ノーマライザの設定は `CorpusCleaner.scala` を直接変更する必要がある

todo: read from config file, or extend cli option

### args

`--input`: input corpus. path to the file / dir (load all files in the dir). Multiple input is allowed (ex. `--input ./file.a ./input_dir/ ./and_more/*.txt`).
each file should be: "\n\n" splitted documents that consists of "\n" splitted sentences.

`--output`: spark output dir (default ./out). need to be empty (if exists).

`--ngwords`: ng-word list (optional). new-line splitted ng-word list (see [chitra ngwords](https://github.com/WorksApplications/SudachiTra/blob/main/pretraining/bert/resources/ng_words.txt)).

## MinHashDeduplicator

類似文書を削除する。

[Deduplicating Training Data Makes Language Models Better](https://arxiv.org/abs/2107.06499) における NearDup の再現だが以下の点で異なることに注意。

- spark.ml の MinHashLSH は OR-amplification ([参考](https://en.wikipedia.org/wiki/Locality-sensitive_hashing#:~:text=%5Bhow%3F%5D-,Amplification,-%5Bedit%5D)) のみの実装のため、ハイパーパラメータ b/r の r のみしか設定できない
- LSH 出力の類似ペア候補に対する exact edit similarity の計算及びフィルタを行っていない

```
spark-submit --class org.sample.corpus.CorpusCleaner \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=./data/nwjc/* --output=./out \
    --mode C \
    --ngram 5 \
    --num-tables 100 \
    --join-thr 0.1
```

### args

- `--input`, `--output`: Same as CorpusCleaner.
- `--save-stats`: Set to output a parquet with document and duplication idx column.
- `--mode`: Sudachi split mode (A/B/C).
- `--ngram`: n of n-gram, used to convert document to a set of n-grams.
- `--num-tables`: Number of hash tables for LSH. The parameter `r` of the reference paper.
- `--join-thr`: Threshold of document distance (0-1). Pairs with distance lower than this is kept.
