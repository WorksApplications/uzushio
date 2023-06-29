package com.worksap.nlp.uzushio.lib.html

import org.apache.tika.parser.html.HtmlMapper

import java.util.Locale

/** Mapper class that provides all tags to handler.
  *
  * With this class set in context, handler can recognize tags specific to html
  * s.t. div, br, etc. ref:
  * https://stackoverflow.com/questions/19368018/parsing-html-elements-in-apache-tika
  */
class AllTagMapper extends HtmlMapper {
  override def mapSafeElement(name: String): String = name.toLowerCase(Locale.ROOT)

  override def isDiscardElement(name: String): Boolean = false

  override def mapSafeAttribute(
      elementName: String,
      attributeName: String
  ): String = attributeName.toLowerCase(Locale.ROOT)
}
