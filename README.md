# Corpus cleaning

大規模コーパス (e.g. Wikipedia, NWJC, etc.) の整形のためのツール

Apach Spark (scala) ベース

## Goal of this project

It is known that ML model training with clean corpus is efficient / achieve high performance [].
There are many many corpus cleaning methods, and suitable sets of them for each nlp tasks.

In this repository, we try to:

- provide a common corpus cleaning tool
  - reduce the re-implementation cost
  - share same implemantation for the compatibility
- keep it efficient (high-speed), flexible and updated
  - it should be able to used in every kind of nlp projects

## ref

- [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn)
  - preprocess in chitra pretraining
- [Deduplicating Training Data Makes Language Models Better](https://arxiv.org/abs/2107.06499)

# setup

You need to install apach-spark, scala (sbt), and sudachi dictionary file.

## apach spark

[download](https://spark.apache.org/downloads.html).

The version should be: `spark 3.2.1` + `Scala 2.12.15`.

check if `spark-submit --version` works.

## scala (sbt)

[download](https://www.scala-sbt.org/download.html).

Note that you should use compatible version to the one used by spark.

## sudachi

[download](http://sudachi.s3-website-ap-northeast-1.amazonaws.com/sudachidict/) sudachi dictionary file (.dict) and place in the root dir.

todo: specify dict by config/args

## other

You may also need a Java environment.

# run

## build

root (`build.sbt`のあるディレクトリ) にて `sbt assembly` でコンパイル

`./target/scala-[version]/` 以下に jar が生成される

### note

- 開発段階などでコンパイルを繰り返すなら `sbt` で sbt-shell を起動しておいた方が速い
- `sbt compile` ではなく `sbt assembly`を使う
  - spark に投げる用に必要ライブラリ等もまとめてバンドルする
    - なお spark は除外する必要あり
  - `./project/plugins.sbt` にてコマンド追加している

## submit

`spark-submit` に jar とオプションを投げる：

```
spark-submit --class [class name] [path/to/jar_file] [args for cli]
```

ex.

```
spark-submit --class org.sample.corpus.CorpusCleaner \
    ./target/scala-2.12/CorpusProcessing-assembly-0.1.jar \
    --input=../data/*.txt --output=./out
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
