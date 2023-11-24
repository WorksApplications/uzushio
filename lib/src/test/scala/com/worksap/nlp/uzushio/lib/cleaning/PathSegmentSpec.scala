package com.worksap.nlp.uzushio.lib.cleaning

import org.scalatest.freespec.AnyFreeSpec

class PathSegmentSpec extends AnyFreeSpec{
  "PathSelector" - {
    "parses selector without classes or id" in {
      val sel = PathSegment.parse("test")
      assert(sel.tag == "test")
      assert(sel.id == null)
      assert(sel.classes.isEmpty)
      assert(sel.toString == "test")
    }

    "parses selector without classes and with id" in {
      val sel = PathSegment.parse("test#id")
      assert(sel.tag == "test")
      assert(sel.id == "id")
      assert(sel.classes.isEmpty)
      assert(sel.toString == "test#id")
    }

    "parses selector with one class and without id" in {
      val sel = PathSegment.parse("test.clz1")
      assert(sel.tag == "test")
      assert(sel.id == null)
      assert(sel.classes == Seq("clz1"))
      assert(sel.toString == "test.clz1")
    }

    "parses selector with two classes and without id" in {
      val sel = PathSegment.parse("test.clz1.clz2")
      assert(sel.tag == "test")
      assert(sel.id == null)
      assert(sel.classes == Seq("clz1", "clz2"))
      assert(sel.toString == "test.clz1.clz2")
    }

    "parses selector with two classes and with id" in {
      val sel = PathSegment.parse("test.clz1.clz2#id")
      assert(sel.tag == "test")
      assert(sel.id == "id")
      assert(sel.classes == Seq("clz1", "clz2"))
      assert(sel.toString == "test.clz1.clz2#id")
    }

    "parses selector with two classes and with id inside other string" in {
      val sel = PathSegment.parse("foo test.clz1.clz2#id test.clz#id2", 4, 21)
      assert(sel.tag == "test")
      assert(sel.id == "id")
      assert(sel.classes == Seq("clz1", "clz2"))
      assert(sel.toString == "test.clz1.clz2#id")
    }

    "parses path of two elements" in {
      val path = PathSegment.parsePath("body>li.test")
      assert(path.size == 2)
      assert(path(0).tag == "body")
      assert(path(1).tag == "li")
      assert(path(1).classes == Seq("test"))
    }
  }

}
