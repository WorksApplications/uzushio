package com.worksap.nlp.uzushio.lib.utils

import org.scalatest.freespec.AnyFreeSpec

class RowBufferSpec extends AnyFreeSpec {
  "RowBuffer" - {
    "single item can be deleted" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(5)
      assert(buf.size() == 1)
      val item = buf.removeElementAt(0)
      assert(item == 5)
      assert(buf.size() == 0)
      assertThrows[IllegalArgumentException](buf.removeElementAt(0))
    }

    "removing item with invalid index throws an exception" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(5)
      assert(buf.size() == 1)
      assertThrows[IllegalArgumentException](buf.removeElementAt(1))
    }

    "works when removing last item of two" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(2)
      buf.addToBuffer(3)
      assert(buf.size() == 2)
      assert(buf.removeElementAt(1) == 3)
      assert(buf.size() == 1)
      assert(buf.get(0) == 2)
    }

    "works when removing first item of two" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(2)
      buf.addToBuffer(3)
      assert(buf.size() == 2)
      assert(buf.removeElementAt(0) == 3)
      assert(buf.size() == 1)
      assert(buf.get(0) == 3)
    }

    "works when removing first item of three" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(2)
      buf.addToBuffer(3)
      buf.addToBuffer(4)
      assert(buf.size() == 3)
      assert(buf.removeElementAt(0) == 4)
      assert(buf.size() == 2)
      assert(buf.get(0) == 4)
      assert(buf.get(1) == 3)
    }

    "works when removing second item of three" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(2)
      buf.addToBuffer(3)
      buf.addToBuffer(4)
      assert(buf.size() == 3)
      assert(buf.removeElementAt(1) == 4)
      assert(buf.size() == 2)
      assert(buf.get(0) == 2)
      assert(buf.get(1) == 4)
    }

    "works when removing third item of three" in {
      val buf = new RowBuffer[Int]()
      buf.addToBuffer(2)
      buf.addToBuffer(3)
      buf.addToBuffer(4)
      assert(buf.size() == 3)
      assert(buf.removeElementAt(2) == 4)
      assert(buf.size() == 2)
      assert(buf.get(0) == 2)
      assert(buf.get(1) == 3)
    }
  }
}
