package com.worksap.nlp.uzushio.lib.utils

import org.apache.commons.io.IOUtils

trait ClasspathAccess {
  def classpathBytes(name: String): Array[Byte] = {
    val resource = getClass.getClassLoader.getResource(name)
    IOUtils.toByteArray(resource)
  }
}
