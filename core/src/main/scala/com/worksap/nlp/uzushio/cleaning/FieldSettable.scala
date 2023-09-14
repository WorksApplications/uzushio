package com.worksap.nlp.uzushio.cleaning

/** Set class field via setField method.
  *
  * T must be the type of the class this is implemented with, e.g. `class MyClass extends
  * FieldSettable[MyClass]`.
  */
trait FieldSettable[T] {
  def setFields(map: Map[String, Any]): T = {
    for ((k, v) <- map) setField(k, v)
    this.asInstanceOf[T]
  }

  def setField(key: String, value: Any): T = {
    this.getClass.getDeclaredFields.find(_.getName == key) match {
      case Some(field) => {
        field.setAccessible(true)
        field.set(this, value)
      }
      case None => throw new IllegalArgumentException(s"No field named $key")
    }
    this.asInstanceOf[T]
  }
}
