package com.worksap.nlp.uzushio.lib.utils

import org.apache.spark.sql.Dataset

object BuilderSyntax {
  implicit class BuilderOps[T](val o: T) extends AnyVal {
    @inline def ifEnabled(cond: Boolean)(fn: T => T): T = {
      if (cond) fn(o) else o
    }
  }

}
