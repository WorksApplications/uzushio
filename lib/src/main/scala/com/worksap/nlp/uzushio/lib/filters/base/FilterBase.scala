package com.worksap.nlp.uzushio.lib.filters.base

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}

/** All filter classes extend from this trait. They must have single public
 * constructor. The framework will handle passing arguments from the config
 * files automatically and will use default arguments correctly (pass value
 * from config, then default parameter if config does not have a parameter with
 * the same name).
 *
 * **On filter functions**. Filtering functions should not remove paragraphs
 * from documents. Instead they should mark paragraph or a document "to delete"
 * with a marker object which should contain the reason of deletion. The marker
 * object can be a string or any JVM object with toString method overriden. The
 * implementation of `toString` should not contain any spaces or other
 * characters which could cause problems in filesystem paths.
 */
trait FilterBase extends Serializable

/** Paragraph-level filter which considers all paragraphs independently. Mark
 * [[Paragraph.remove]] field with the marker object.
 *
 * @see
 * [[FilterBase]] about marker objects
 */
trait ParagraphFilter extends FilterBase {
  def checkParagraph(p: Paragraph): Paragraph
}

/** Document-level filter. Should not remove any paragraphs. Instead, mark
 * [[Document.remove]] or [[Paragraph.remove]] with a marker object.
 *
 * @see
 * [[FilterBase]] about marker objects
 */
trait DocFilter extends FilterBase {
  def checkDocument(doc: Document): Document
}
