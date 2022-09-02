package org.sample.corpus.warc

import org.apache.tika.parser.html.HtmlMapper

/** Mapper class that provides all tags to handler.
  *
  * With this class set in context, handler can recognize tags specific to html
  * s.t. div, br, etc. ref:
  * https://stackoverflow.com/questions/19368018/parsing-html-elements-in-apache-tika
  */
class AllTagMapper extends HtmlMapper {
  override def mapSafeElement(name: String): String = {
    return name.toLowerCase();
  }

  override def isDiscardElement(name: String): Boolean = {
    return false;
  }

  override def mapSafeAttribute(
      elementName: String,
      attributeName: String
  ): String = {
    return attributeName.toLowerCase();
  }
}
