package com.worksap.nlp.uzushio.lib.cleaning

import com.typesafe.config.Config
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import org.apache.commons.lang3.StringUtils

import java.lang.reflect.{Constructor, Parameter}

/**
 *
 * @param path      html path of the paragraph, separated by >, with . for classes and # for ids
 * @param text      text content of the paragraph with link content possibly inside STX/ETX character pairs
 * @param index     index of the paragraph in the document, starting from 0
 * @param exactFreq number of occurrences of the paragraph with the same hash value in the corpus
 * @param nearFreq  number of occurrences of the near-duplicate paragraphs
 * @param remove    set this field to not-null value to remove this document
 */
case class Paragraph(
    path: String,
    text: String,
    index: Int = 0,
    exactFreq: Int = 1,
    nearFreq: Int = 1,
    remove: AnyRef = null
                    ) {
  def renderInto[T <: Appendable](bldr: T): T = {
    if (path != null && path.nonEmpty) {
      bldr.append(path)
      bldr.append(Paragraphs.HTML_PATH_SEPARATOR)
    }
    bldr.append(text)
    bldr
  }
}

case class Document(
                     paragraphs: IndexedSeq[Paragraph],
    remove: AnyRef = null
) {
  def removeWhen(toRemove: Boolean, remover: AnyRef): Document = {
    if (toRemove) copy(remove = remover) else this
  }

  def render(): String = {
    val bldr = new java.lang.StringBuilder()
    val iter = paragraphs.iterator
    var first = true
    while (iter.hasNext) {
      if (!first) {
        bldr.append("\n\n")
      }
      iter.next().renderInto(bldr)
      first = false
    }
    bldr.toString
  }
}

object Document {
  def parse(s: String): Document = {
    val paragraphs = StringUtils.split(s, "\n\n")
    val parObjects = paragraphs.map { text =>
      val (path, content) = Paragraphs.splitPath(text)
      Paragraph(path, content)
    }
    Document(parObjects)
  }
}

trait FilterBase extends Serializable

trait ParagraphFilter extends FilterBase {
  def checkParagraph(p: Paragraph): Paragraph
}

trait DocFilter extends FilterBase {
  def checkDocument(doc: Document): Document
}

class PerParagraphFilter(val filter: ParagraphFilter) extends DocFilter {
  override def checkDocument(doc: Document): Document =
    doc.copy(paragraphs = doc.paragraphs.map(filter.checkParagraph))
}

class Pipeline extends Serializable {}

object Pipeline {

  def findFilterClass(clzName: String): Class[_] = {
    getClass.getClassLoader.loadClass(clzName)
  }

  private def getParam(
                        clz: Class[_],
                        cfg: Config,
                        par: Parameter,
                        index: Int
                      ): AnyRef = {
    if (!cfg.hasPath(par.getName)) {
      val defFnName = "$lessinit$greater$default$" + index
      try {
        val defMethod = clz.getMethod(defFnName) // should be static
        return defMethod.invoke(null)
      } catch {
        case _: NoSuchMethodException =>
          throw new IllegalArgumentException(
            s"could not instantiate $clz, ${par.getName} did not configured or have default value"
          )
      }
    }

    val tpe = par.getType
    if (tpe == classOf[String]) {
      cfg.getString(par.getName)
    } else if (tpe == classOf[Int] || tpe == classOf[java.lang.Integer]) {
      cfg.getInt(par.getName).asInstanceOf[AnyRef]
    } else if (tpe == classOf[Float] || tpe == classOf[java.lang.Float]) {
      cfg.getDouble(par.getName).toFloat.asInstanceOf[AnyRef]
    } else if (tpe == classOf[Double] || tpe == classOf[java.lang.Double]) {
      cfg.getDouble(par.getName).asInstanceOf[AnyRef]
    } else {
      throw new IllegalArgumentException(s"type $tpe is not supported yet")
    }
  }

  def tryInstantiate(
                      clz: Class[_],
                      ctor: Constructor[_],
                      cfg: Config
                    ): DocFilter = {
    val partypes = ctor.getParameters
    val args = new Array[AnyRef](partypes.length)

    var i = 0
    while (i < partypes.length) {
      val par = partypes(i)
      val arg = getParam(clz, cfg, par, i + 1)
      args(i) = arg
      i += 1
    }

    ctor.newInstance(args: _*) match {
      case f: DocFilter       => f
      case f: ParagraphFilter => new PerParagraphFilter(f)
      case _                  => throw new Exception("")
    }
  }

  def instantiateFilter(cfg: Config): DocFilter = {
    val tpe = cfg.getString("class")
    val clz = findFilterClass(tpe)
    val ctors = clz.getConstructors
    val iter = ctors.iterator
    while (iter.hasNext) {
      val ctor = iter.next()
      val instance = tryInstantiate(clz, ctor, cfg)
      if (instance != null) {
        return instance
      }
    }
    throw new Exception(
      "could not find a suitable constructor to create filter " + cfg
    )
  }
}
