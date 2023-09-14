package com.worksap.nlp.uzushio.lib.utils

import org.apache.spark.SparkContext

object Resources {
  implicit class AutoClosableResource[T <: AutoCloseable](val x: T) extends AnyVal {
    @inline
    def use[X](fn: T => X): X =
      try {
        fn(x)
      } finally {
        x.close()
      }
  }

  implicit class SparkContextResource(val x: SparkContext) extends AnyVal {
    @inline
    def use[X](fn: SparkContext => X): X =
      try {
        fn(x)
      } finally {
        x.stop()
      }
  }

}
