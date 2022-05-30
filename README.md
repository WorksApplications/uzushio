# Corpus cleaning

大規模コーパス (e.g. Wikipedia, NWJC, etc.) の整形を目的としたツール

Apach Spark (scala) ベース

# setup

コンパイル及び実行のために apach spark と scala のインストールが必要

また内部で sudachi を使用するため、辞書ファイル(.dict)を root に配置する必要がある

## spark

https://spark.apache.org/downloads.html

適宜インストールしパスを通す

`spark-submit --version` が動けば OK

`spark 3.2.1` + `Scala 2.12.15` で動作確認している

## scala (sbt)

https://www.scala-sbt.org/download.html

こちらも適宜インストールする

spark が参照している scala バージョンに合わせること

## sudachi

sudachi 辞書ファイルをダウンロードしルートに配置する
todo: 位置の path による指定

## other

別途 Java が必要？

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

## CorpusSelector

入力コーパスからシード文書に類似するものを選別する

現在は特徴量化は tfidf 、類似度計算は cos-similarity ベースの Locality-Sensitive-Hashing による

todo: read from config file

todo: add other vectorizer / similarity measure

### args

`--corpus-doc`: 選別の対象とするコーパス

`--seed-doc`: 選別の基準とするコーパス

`--output`: output dir (default ./out).

`--mode`: sudachi split mode (default C).

`--num-tf-features`: dimension of tf feature vector (default 1000).

`--bucket-length`: LSH bucket length (default 2).

`--num-tables`: number of LSH hash table (default 3).

`--num-samples`: number of documents to output (default 20).

### sub-class

CorpusSelector の各段階について個別で実行できるようにしている

詳細は各.scala ファイルを参照のこと（Conf class にて cli 設定を行っている）

- DocumentIO
  - 後段の処理のため生コーパスの各文書に id を振る / 取り除く
- Vectorizer
  - 各文書の特徴量化を行う
- SimilaritySelector
  - 特徴量の類似度による選別を行う
