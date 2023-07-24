package com.worksap.nlp.uzushio.lib.utils

import org.apache.spark.sql.Dataset

object DataframeUtils {
  implicit class DatasetOps[T](val df: Dataset[T]) extends AnyVal {
    @inline def optOp(x: Boolean, fn: Dataset[T] => Dataset[T]): Dataset[T] = {
      if (x) fn(df) else df
    }
  }

}
