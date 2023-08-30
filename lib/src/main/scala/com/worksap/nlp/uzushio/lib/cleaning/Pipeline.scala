package com.worksap.nlp.uzushio.lib.cleaning

import com.typesafe.config.Config

import java.lang.reflect.{Constructor, Parameter}

case class Paragraph(
    path: String,
    text: String,
    remove: AnyRef = null
)

case class Document(
    paragraphs: Seq[Paragraph],
    remove: AnyRef = null
) {
  def removeWhen(toRemove: Boolean, remover: AnyRef): Document = {
    if (toRemove) copy(remove = remover) else this
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

  private def getParam(cfg: Config, par: Parameter): AnyRef = {
    val tpe = par.getType
    if (tpe == classOf[String]) {
      cfg.getString(par.getName)
    } else if (tpe == classOf[Int] || tpe == classOf[Integer]) {
      cfg.getInt(par.getName).asInstanceOf[AnyRef]
    } else {
      throw new IllegalArgumentException("not supported yet")
    }
  }

  def tryInstantiate(ctor: Constructor[_], cfg: Config): DocFilter = {
    val partypes = ctor.getParameters
    val args = new Array[AnyRef](partypes.length)

    var i = 0
    while (i < partypes.length) {
      val par = partypes(i)
      val arg = getParam(cfg, par)
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
      val instance = tryInstantiate(ctor, cfg)
      if (instance != null) {
        return instance
      }
    }
    throw new Exception(
      "could not find a suitable constructor to create filter " + cfg
    )
  }
}
