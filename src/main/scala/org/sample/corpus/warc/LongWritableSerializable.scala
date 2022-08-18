package org.sample.corpus.warc

import org.apache.hadoop.io.LongWritable;
import java.io.Serializable

/* Serializable wrapper of Hadoop LongWritable class.
 *
 * ref: https://issues.apache.org/jira/browse/SPARK-2421
 */
class LongWritableSerializable extends LongWritable with Serializable
