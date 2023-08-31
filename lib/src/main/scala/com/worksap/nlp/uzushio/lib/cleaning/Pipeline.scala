package com.worksap.nlp.uzushio.lib.cleaning

import com.typesafe.config.Config
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import org.apache.commons.lang3.StringUtils

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
