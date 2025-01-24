# About Uzushio

Uzushio is a web-scale corpus text extraction and preprocessing tool.
It uses Spark and written mostly in Scala.

## Goal of this project

LLMs and foundational models require huge clean corpus for training.
There are many cleaning methods, and each nlp tasks have suitable sets of them.

Uzushio has the following goals:

- provide a common corpus cleaning tool
  - reduce the implementation cost
  - share same implementation for the compatibility
- keep it efficient, flexible and updated
  - it should be able to used in every kind of nlp projects (which uses large corpus)
- support AWS EMR

# Setup

You need to install Apache Spark, sbt, and Sudachi dictionary file.

## Apache Spark (if running locally, if you want to use AWS EMR you do not need it)

Download it from the [official page](https://spark.apache.org/downloads.html). 
You can put it in PATH or use absolute paths for Spark startup scripts to launch Uzushio. 

At the moment uzushio is built against: `spark 3.5.3` + `Scala 2.12.*`.

Check if `spark-submit --version` works.

## Scala (sbt)

Download sbt (latest 1.* version) from the [official page](https://www.scala-sbt.org/download.html) and make it usable.
You can often install one from OS package manager.

### JDK

You need JDK 8+ to run sbt and Spark. Usually, you can install it from a package manager.
However, if you plan to launch Uzushio in EMR, you either need to use JDK 8 to compile Uzushio,
or assemble a custom image for EMR to use JDK 11+ inside EMR (currently it uses JDK 8),
otherwise you will get MethodNotFoundErrors.

# Building

## Build fat JAR for launching

The easiest way to use the tool — build fat JAR with all required dependencies.

Execute `sbt assembly` from project root directory.
You will have output jar under `./target/scala-[version]/`.

### For development

- For development, it is better to launch sbt shell via `sbt` and launch commands inside the sbt.
- Launch classes located in `com.worksap.nlp.uzushio.lib.runners` are designed to be launched without `spark-submit` script
  - You can use your IDE functionality which will set up correct classpath or use `lib/run` command in the sbt shell
- You can do compilation via `assembly` command
  - Also, see [sbt docs](https://www.scala-sbt.org/1.x/docs/Running.html)

## Running via spark environment

Use `spark-submit` with jar built by previous steps：

```
spark-submit [spark options] --class [class name] [path/to/jar_file] [class options]
```

Also, see [`spark-submit` docs](https://spark.apache.org/docs/latest/submitting-applications.html)

# Runner classes

Note that all paths can be on any Hadoop-supported filesystem.
For S3, specify `s3://` paths if you **use EMR** and `s3a://` paths if you **do not** use EMR.

Classes in `com.worksap.nlp.uzushio.main` are designed to run from `spark-submit` script

## `com.worksap.nlp.uzushio.main.ExtractTextFromWarc`

Extract paragraphs from html documents inside WARC collection.
Paragraphs are guessed on best effort basis from html structure (e.g. div, p tags).

#### Run Example
```bash
spark-submit \
    --class=com.worksap.nlp.uzushio.main.ExtractTextFromWarc \
    --master='local[*]' \
    /path/to/uzushio-assembly-0.1-SNAPSHOT.jar \
    --input=/path/to/warc/files \
    --output=/path/to/output \
    --language=ja,ru,ko,zh 
```

#### Arguments
* `--input` path to input data, can be any Hadoop-supported filesystem 
* `--output` path to where output data will be written, in parquet format
* `--language` extract documents with following languages. Languages with latin script are **NOT** supported.
* `--max-partitions` (default 2000) produce at most this number of partitions in output data.
* `--compression` (default zstd) compress parquet data with this compression algorithm. See [supported algorithms](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option).

# License

Copyright (c) 2024 Works Applications Co., Ltd. All rights reserved.

Uzushio is distributed by [Works Applications Co.,Ltd.](https://www.worksap.co.jp/) under [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

# References

## WARC

- [warc format specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0)
  - Note that latest version is v1.1 but CC uses v1.0
- [org.archive.io](http://crawler.archive.org/apidocs/org/archive/io/package-summary.html)
  - Iterates over warc records
- [com.martinkl.warc](https://github.com/ept/warc-hadoop)
  - Loads warc file using Hadoop API

## Other

- [chiTra の前処理について](https://docs.google.com/document/d/1colWQgSc22rzLHKdCH78BgtRLydGMZX-D-FAT6rD8iY/edit#heading=h.msy5fu9l7egn)
  - preprocess in chitra pretraining
- [Deduplicating Training Data Makes Language Models Better](https://arxiv.org/abs/2107.06499)
