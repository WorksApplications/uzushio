package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject
import org.apache.spark.sql.Dataset

/** Split each elements of the document by given delimiter and flatten.
  *
  * Use this to split document/paragraph into paragraph/sentence.
  *
  * @param delim
  *   the delimiter to split each elements.
  */
class SplitElement(delim: String = "\n")
    extends Transformer
    with FieldSettable[SplitElement] {
  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._
    ds.map(_.flatMap(_.split(delim)))
  }
}

object SplitElement extends FromConfig {
  override def fromConfig(conf: ConfigObject): SplitElement = {
    val args = Map[String, Option[Any]]("delim" -> conf.getAs[String]("delim"))
      .collect { case (k, Some(v)) => k -> v }
    new SplitElement().setFields(args)
  }
}

/** Split element into sentences. */
class SplitIntoSentence extends SplitElement(delim = "\n")
object SplitIntoSentence extends FromConfig {
  override def fromConfig(conf: ConfigObject): SplitIntoSentence =
    new SplitIntoSentence()
}

/** Split element into paragraphs. */
class SplitIntoParagraph extends SplitElement(delim = "\n\n")
object SplitIntoParagraph extends FromConfig {
  override def fromConfig(conf: ConfigObject): SplitIntoParagraph =
    new SplitIntoParagraph()
}
