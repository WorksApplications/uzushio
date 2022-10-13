# WARC

Handle WARC (Web ARChive) file, such that NWJC or CommonCrawl data.

## processing step

- warc record
    - load records from warc format file into spark RDD.
    - use "non-truncated" and "warc-respose type" record, with contents type "application/http".
- http response
    - parse record body as http response.
    - skip if its contents type is not "text/html".
- html file
    - parse response body as html using Tika.
        - skip when failed to detect encoding from http header or html meta-tag.
    - split text into paragraphs based on dom structure (see `ParagraphHandler`).
- text cleaning (minimum)
    - trim each sentences and remove empty lines/paragraphs/documents.

## run

sample:

```bash
input="/data/nwjc/01warc/NINJAL-2014-4Q/*.warc.gz"
output="/data/works/nwjc/warc_processed"

nice -n 19 spark-submit \
    --class org.sample.corpus.warc.WarcToDocument \
    ./target/scala-2.12/CorpusCleaning-assembly-0.1.jar \
    --input ${input} \
    --output ${output} \
    &> "./logfile"
```

This will output parquet files with "WARC-Target-URI" column (contains url) and "document" column (contains extracted text).
By default the extracted documents consists of paragraphs delimited by "\n\n" (any other empty lines are removed).

This also outputs full extraced data to `${output}_fulldata` directory (suppress with `--result-only` option).
It contains:

- Warc Headers (Map[String, String])
- Http response headers (Array[List[String, String]])
- Meta data extracted by Tika (Map[String, String])
- Raw http response body (i.e. html)
- Document extracted

It takes about 15 hours to process all NWJC warc files (NINJAL-2014-4Q) with lserv65 server.
Output file size is 260G (text w/ url) (ref: NINJAL-2014-4Q: 850G, fulldata output: 1.2T).

## post processing

The output texts (with url) are not clean enough. Apply `CorpusCleaner` with `--config "warc"` before appling MinHash deduplication.
It will do followings:

- Japanese filtering
    - filter out non-clean-japanese text, i.e. other languages or 文字化け.
    - current alg is based on character type counting.
- Paragraph deduplication
    - remove same paragraphs over all documents.
        - It is expected that this will remove template patterns, utilizing paragraph information.
- Rm short documents and concat paragraphs
    - let text format be same to `04textwourl` (empty line separated documents).
- Then apply `chitra` + `rmTemplate` compatible cleaning

# known issues

## NWJC files

we now skip files with `.warc.gz.open` extension, s.t.

```
NINJAL-2014-4Q-0-20141023060508543-00098-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz.open
```

processing following NWJC file cause OOMem error.

```
NINJAL-2014-4Q-0-20141009201544561-00057-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-1-20141027224725889-00057-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-2-20141028000402869-00057-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-5-20141109091900026-00057-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-6-20141109093235033-00048-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-8-20141225105319606-00054-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-8-20141225183733967-00056-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
NINJAL-2014-4Q-8-20141226152801007-00062-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz
```

error message:
```
org.apache.spark.SparkException: Task failed while writing rows.
	at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:500)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:321)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$write$16(FileFormatWriter.scala:229)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
	at org.apache.spark.scheduler.Task.run(Task.scala:131)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:506)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1462)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:509)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.lang.OutOfMemoryError: Requested array size exceeds VM limit
	at java.lang.StringCoding.encode(StringCoding.java:350)
	at java.lang.String.getBytes(String.java:941)
	at org.apache.spark.unsafe.types.UTF8String.fromString(UTF8String.java:142)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.serializefromobject_doConsume_0$(Unknown Source)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:759)
	at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:304)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$$$Lambda$2929/1423358696.apply(Unknown Source)
	at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1496)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:311)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$write$16(FileFormatWriter.scala:229)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$$$Lambda$2790/2138055926.apply(Unknown Source)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
	at org.apache.spark.scheduler.Task.run(Task.scala:131)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:506)
	at org.apache.spark.executor.Executor$TaskRunner$$Lambda$2658/1453419079.apply(Unknown Source)
	... 5 more
```

processing following NWJC file does not finish in expected time.

```
NINJAL-2014-4Q-8-20141224150017006-00048-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz # takes too much time
NINJAL-2014-4Q-9-20141224162102808-00048-8451~ccd-lserv-41.ninjal.ac.jp~8443.warc.gz # takes too much time
```

### charset detection

Current implementation only check http response header and meta-tag in html to detect encodings.
In the case no encodings are detected, it just skip the record.
It is also problematic to rely on them, since sometimes wrong information is provided.

We should introduce some encoding detection tool for this problem.

error message logged when encoding detection failed:
`org.apache.tika.exception.TikaException: Failed to detect the character encoding of a document`.

### error recovery

In the case of parse error (http/html), the record is just skipped.
It may be possible to recover in some of those cases.

same examples of errors:
- `HttpResponseParser: error parsing http response: org.apache.hc.core5.http.ParseException: Invalid header; error at offset 3: <P3P : CP="ALL CURa ADMa DEVa TAIa OUR BUS IND PHY ONL UNI PUR FIN COM NAV INT DEM CNT STA POL HEA PRE LOC OTC">`
- `HttpResponseParser: error parsing http response: org.apache.hc.core5.http.ParseException: Invalid header: <<html>>`
- `HttpResponseParser: error parsing http response: org.apache.hc.core5.http.ParseException: Invalid header; error at offset 8: <location : index.php>`
- `HttpResponseParser: error parsing http response: org.apache.hc.core5.http.ParseException: Invalid header; error at offset 6: <Server : Live 5>`
- `HttpResponseParser: error parsing http response: org.apache.hc.core5.http.ParseException: Invalid header; error at offset 7: <keyword : >`
- `error during tika parsing: java.lang.StringIndexOutOfBoundsException: String index out of range: -1`
  - this seems to happend at the end of each warc file.

# statistics

We record document count for the reference.

__v1__

- WARC responce records (application/http): --
- HTML records (http responces with content-type text/html): 82583838
- Documents correctly parsed: 72757367
- Documents skipped due to error: 9826471

It fails to parse about 11.9% of HTMLs.
Also note that some WARC files are skipped due to the error (see known issues section).
