# Uzushio チュートリアル

本文書では、具体的なコマンドを提示しつつ Uzushio の利用方法を紹介します。

基本的な処理の説明を目標とし、計算機環境やデータについては最小限のものを扱います。
また各フィルターの詳細等も扱いません。

## セットアップ

### spark + scala

コードのビルドのために scala-sbt, 実行には Apache Spark を使用します。
それぞれ公式サイトからダウンロードし、使用可能な状態にします

sbt:

```bash
# https://www.scala-sbt.org/download/ から v1.* を取得
wget https://github.com/sbt/sbt/releases/download/v1.10.0/sbt-1.10.0.tgz
tar -xvf sbt-1.10.0.tgz

# 適宜パスを通す
export PATH=$PATH:/path/to/sbt/bin

# 確認
sbt --version
```

spark:

```bash
# https://spark.apache.org/downloads.html から scala 2.12 用を取得
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvf spark-3.5.1-bin-hadoop3.tgz

# 適宜パスを通す
export PATH=$PATH:/path/to/spark-3.5.1-bin-hadoop3/bin

# 確認
spark-submit --version
```

### ビルド

リポジトリのルートで `sbt assembly` コマンドを実行し、 Uzushio をビルドします。
`core/target/scala-2.12/` 以下に jar が生成されます

```bash
cd uzushio/
sbt assembly

# 確認
ls core/target/scala-2.12/
```

### Common Crawl

処理対象として、CommonCrawl から WARC (Web ARChive) 形式データを取得します。
ここでは 2 ファイルのみを用いることにします。

```bash
# 2024-22 から先頭 2 ファイルを取得
wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-22/segments/1715971057216.39/warc/CC-MAIN-20240517233122-20240518023122-00000.warc.gz
wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-22/segments/1715971057216.39/warc/CC-MAIN-20240517233122-20240518023122-00001.warc.gz

# 適当な箇所に配置
mkdir cc-data
mv CC-MAIN-*.warc.gz cc-data/
```

## 1. WARC ファイルからのデータ抽出

### 処理内容

Uzushio は 2 つの主実行クラスを持ちます。
"ExtractTextFromWarc" は WARC ファイルからのテキスト抽出を行います。

大まかな処理内容は以下の通りです。

- WARC から HTTP Response のデータを抽出
- 言語およびエンコーディングを判定
- HTML データを抽出
- HTML タグを元に段落に分割しつつテキストデータを抽出

### 実行

実行には `com.worksap.nlp.uzushio.main.ExtractTextFromWarc` クラスを用います。

引数は [ExtractParagraphsFromWARC.scala](../lib/src/main/scala/com/worksap/nlp/uzushio/lib/runners/ExtractParagraphsFromWARC.scala) の `ConfigParser` で定義されています。
主な引数として以下があります。

- input: warc ファイルへのパス
- output: 出力ディレクトリのパス
- language: 抽出対象とする言語 (',' 区切り)

output に指定したディレクトリ以下に、抽出されたデータが各言語ごとに parquet 形式で出力されます。
出力 parquet は ['docId', 'url', 'charset', 'text', 'date'] をカラムに持ちます（[WarcEntryParser.scala](../lib/src/main/scala/com/worksap/nlp/uzushio/lib/warc/WarcEntryParser.scala) の `CrawlContent` で定義）。
"text" カラムのテキストデータは段落ごとに空行 ("\n\n") で区切られており、さらに各段落の前にその段落へのセレクタが "0xc1" で区切って記録されています。

#### example

```bash
spark-submit \
    --class=com.worksap.nlp.uzushio.main.ExtractTextFromWarc \
    --master='local[*]' \
    ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
    --input=./cc-data/*.warc.gz \
    --output=output_extracted/ \
    --language=ja
```

ログ出力から抽出された文書数を確認できます。
今回対象とした 2 ファイルには日本語文書は計 272 件含まれていました: `INFO WarcTextExtraction: input docs=59536, processed=272`

## 2. テキストの重複削除（sim hash）

### 処理内容

"DeduplicateParagraphs" は一段階目で抽出したテキストのクリーニングを行います。
この処理は大きく重複検出とフィルタリングの 2 段階に分かれています。
本節ではまず重複検出について説明します。

Uzushio は sim hash を用いた重複検出を行います。
これにより完全一致だけでなく、一定以上類似した文書を重複として検出することができます。

内部での処理は以下の３段階となっています。

- "reprHashes"
  - 各段落について sim hash を計算
- "stats"
  - 各段落の重複数を計算
- reassemble & filtering
  - 元テキストに段落単位で重複数情報を付与
  - 別途定義したフィルターにより重複を削除

[デフォルトのフィルタ定義](../lib/src/main/resources/pipeline/all_duplicate_paragraphs.conf) では全ての（類似）重複文書について一つを残して削除を行い、そのほかの処理は行いません。

### 実行

実行には `com.worksap.nlp.uzushio.main.DeduplicateParagraphs` を用います。

引数は [DeduplicateParagraphs.scala](../lib/src/main/scala/com/worksap/nlp/uzushio/lib/runners/DeduplicateParagraphs.scala) の `ArgParser` で定義されています。
主な引数として以下があります。

- input: `ExtractTextFromWarc` の出力ファイルへのパス
- output: 出力ディレクトリの指定
- execution: 実行ステージのリスト (',' 区切り)
- cache: 途中計算結果の指定
  - 途中保存した結果を利用する場合はここで指定します
- text-only: セレクタを削除しテキストのみを出力する

デフォルトでは上記３段階のうち前２つの計算結果はキャッシュからの読み込みを前提とします。
それらの計算を実行する場合は execution にそれぞれ "reprHashes", "stats" を指定します。
従って全ての段階を一から実行するには、`execution="reprHashes,stats"` を指定します。

execution に "saveReprHashes", "saveStats", "saveReassembled" を指定することで処理をその段階で終了しその時点の結果を出力します。
`cache` にこれにより計算結果を保存したディレクトリを指定することで計算結果を再利用します。

#### example

最初からすべての処理を実行:

```bash
spark-submit \
    --class=com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master='local[*]' \
    ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
    --input=output_extracted/language=ja/ \
    --output=output_dedup/ \
    --execution="reprHashes,stats" \
    --text-only
```

適当な方法で出力の差分をとると、重複する文書３件やその他定型的な段落が削除されていることが確認できます。

hash の計算のみ実行:

```bash
spark-submit \
    --class=com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master='local[*]' \
    ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
    --input=output_extracted/language=ja/ \
    --output=output_hashes/ \
    --execution="reprHashes,saveReprHashes"
```

hash 計算結果をキャッシュから読みつつ stats を計算:

```bash
spark-submit \
    --class=com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master='local[*]' \
    ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
    --input=output_extracted/language=ja/ \
    --cache=output_hashes/ \
    --output=output_stats/ \
    --execution="stats,saveStats"
```

#### 出力概要

"saveReprHashes": ['text', 'signature', 'freq', 'hash', 'reprHash'] をカラムに持つ parquet を出力します。
入力文書群の各段落についてハッシュ値を計算したものです。

"saveStats": ['hash', 'reprHash', 'exactFreq', 'nearFreq'] をカラムに持つ parquet を出力します。
各ハッシュ値に対して完全一致および類似での重複数を計算したものです。

"saveReassembled": ['docId', 'url', 'pos', 'text', 'exactFreq', 'nearFreq'] を key に持つ jsonl を出力します。
入力文書の段落と重複数情報を文書が再構成できるように join したものです。

最終出力: ['docId', 'url', 'charset', 'date', 'text'] をカラムに持つ parquet を出力します。
`ExtractTextFromWarc` での抽出結果にフィルターを適用した結果です。

### 重複情報のマージ

処理対象データが大量にある場合、重複数計算 ("stats") までを個別に実行した後、それらをマージすることで全体の重複数を計算することができます。

分割したデータについてそれぞれ計算・保存した結果を `com.worksap.nlp.uzushio.lib.runners.MergeDedupStats` に渡すことでこれを行います。この時 `intermediate` フラグを追加し、マージのための情報も保存させる必要があることに注意してください。

マージした重複計算結果を "cache" 引数から渡すことで、データ全体での重複数を参照しつつ分割したデータのクリーニングを行うことができます。

example:

```bash
# stats までを個別に計算し、保存
for idx in ("00000" "00001"); do
  spark-submit \
      --class=com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
      --master='local[*]' \
      ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
      --input=output_extracted/language=ja/part-${idx}-*.parquet \
      --output=output_stats_${idx}/ \
      --execution="reprHashes,stats,saveStats" \
      --intermediate
done

# マージ
spark-submit \
    --class=com.worksap.nlp.uzushio.lib.runners.MergeDedupStats \
    --master='local[*]' \
    ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
    --input=output_stats_0000* \
    --output=output_stats_merged
```

## 3. テキストのクリーニング

"DeduplicateParagraphs" の "filters" 引数にフィルター定義を渡すことにより、重複削除以外にも各種のクリーニングを行うことができます。

### フィルター定義の記述

フィルター定義は HOCON（JSON のスーパーセット）で記述します（[com.typesafe.config](https://github.com/lightbend/config) を使用して読み込まれます）。

トップレベルオブジェクトの "filters" キーに、適用するフィルターのリストを指定します。
各フィルターを定義するオブジェクトでは "class" キーで使用するクラス名称を指定し、加えてそのクラスのコンストラクタ引数を適宜指定します。
Uzushio で定義されているフィルタークラスの名称についてはプレフィックス `com.worksap.nlp.uzushio.lib.filters.` は省略することができます。

定義したフィルターパイプラインはリストの先頭から順に適用され、一度フィルターされた文書は以降は無視されます。

フィルタ定義の実装は [Pipeline.scala](../lib/src/main/scala/com/worksap/nlp/uzushio/lib/cleaning/Pipeline.scala)、実装されているフィルターについては [lib/filters](../lib/src/main/scala/com/worksap/nlp/uzushio/lib/filters/) を参照してください。

### 実行

実行は "filters" 引数にフィルター定義ファイルのパスを指定することをのぞき 2. と同様です。

例として以下のフィルター定義を使用します。
内容は、

- 文書長が 50 文字以下のものを削除
- 平仮名の割合が 10% 以下のものを削除
- リンクになっている部分が 50% を超えるものを削除
- 文書中の段落の percentile 位置の重複数を基準に、期待値 expected 文書が残るよう確率的に削除
  - 乱数の seed は各文書から生成されるため、処理結果は決定的です

```json:sample_filters.conf
filters: [
    {"class": "DocLength", "low": 50},
    {"class": "HiraganaRatio", "low": 0.1, "high": 2.0},
    {"class": "LinkCharRatio", "low": 0, "high": 0.5},
    {"class": "DuplicateParagraphs", "limit": 10},
    {"class": "DeduplicateDocumentsPercentile", "expected": 1.5, "percentile": 0.1},
]
```

#### example

stats までの計算はキャッシュから読み込む:

```bash
spark-submit \
    --class=com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master='local[*]' \
    ./core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar \
    --input=output_extracted/language=ja \
    --cache=output_stats/ \
    --output=output_filtered/ \
    --filters=sample_filters.conf \
    --text-only
```

### フィルタリングの可視化

"execution" に "filter-debug" を指定することで、各フィルターの適用状況を確認することができます。
これが指定された場合、通常と同様の処理が行われた後、適用された各フィルターごとにそれによって除去された文書（段落群）を出力します。
フィルタされなかったものは "filter=null" に出力されます。
