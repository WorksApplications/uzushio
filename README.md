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

入力コーパスを整形する。

現状は [sudachitra での整形](https://github.com/WorksApplications/SudachiTra/tree/main/pretraining/bert#2-preprocessing-corpus-cleaning) と同等（のはず）。
(ref: [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn))

現在フィルタ/ノーマライザの設定は `CorpusCleaner.scala` を直接変更する必要がある。(todo: read from config file, or extend cli option)

```
spark-submit --class org.sample.corpus.CorpusCleaner \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=../data/*.txt --output=./out \
    --ngwords ./resources/ng_words.txt
```

### args

- `--input`: input corpus. path to the file / dir (load all files in the dir). Multiple input is allowed (ex. `--input ./file.a ./input_dir/ ./and_more/*.txt`).
  each file should be: "\n\n" splitted documents that consists of "\n" splitted sentences.
- `--output`: spark output dir (default ./out). need to be empty (if exists).
- `--ngwords`: ng-word list (optional). new-line splitted ng-word list (see [chitra ngwords](https://github.com/WorksApplications/SudachiTra/blob/main/pretraining/bert/resources/ng_words.txt)).

## RemoveTempleate

以下の削除を行う

- 重複文書（完全一致）
- 指定したテンプレート文（or 段落）
- 連続して繰り返される同一文（１文のみ残す）

基本的には CorpusCleaner と同設計のため、コンフィグでの動作指定機能を実装する際に合わせて削除される予定。

```
spark-submit --class org.sample.corpus.CorpusCleaner \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input=../data/*.txt --output=./out \
    --substrs ./resources/template_sentences.txt --per-sentence \
    --min-repeat 2
```

### args

- `--input`: input corpus. path to the file / dir (load all files in the dir). Multiple input is allowed (ex. `--input ./file.a ./input_dir/ ./and_more/*.txt`).
  each file should be: "\n\n" splitted documents that consists of "\n" splitted sentences.
- `--output`: spark output dir (default ./out). need to be empty (if exists).
- `--substrs`: The template substring list to remove. 2 new-line splitted texts. (default `resources/template_sentences.txt`)
- `--per-sentence`: If set, remove substring only when it starts and ends at new-line.
- `--min-repeat`: Deduplicate repeating sentences if it repeats more than or equal to this number.

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