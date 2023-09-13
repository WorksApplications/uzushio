package com.worksap.nlp.uzushio.lib.utils

import org.scalatest.freespec.AnyFreeSpec

import scala.annotation.varargs

class MathUtilTest extends AnyFreeSpec {
  import MathUtilTest._

  "MathUtil" - {

    "longFromBytes" - {
      "produces correct long from 0-based 8-length sequence" in {
        val bytes =
          Array[Byte](0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88.toByte)
        val long = MathUtil.longFromBytes(bytes, 0)
        assert(0x8877665544332211L == long)
      }

      "produces correct long from 0-based 7-length sequence" in {
        val bytes = Array[Byte](0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77)
        val long = MathUtil.longFromBytes(bytes, 0)
        assert(0x77665544332211L == long)
      }

      "produces correct long from 2-based 8-length sequence" in {
        val bytes = Array[Byte](
          0x00,
          0x00,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          0x88.toByte
        )
        val long = MathUtil.longFromBytes(bytes, 2)
        assert(0x8877665544332211L == long)
      }

      "produces correct long from 1-based 8-length sequence" in {
        val bytes = Array[Byte](
          0x00,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          0x88.toByte,
          0x00
        )
        val long = MathUtil.longFromBytes(bytes, 1)
        assert(0x8877665544332211L == long)
      }
    }

    "shiftBytesRight" - {
      "works with 3 bytes with shift=5" in {
        val in = byteArray("10000000", "00001000", "00000001")
        val out = MathUtil.rotateBitsRight(in, 5)
        assert(out === byteArray("00001100", "00000000", "01000000"))
      }

      "works with 3 bytes with shift=8" in {
        val in = byteArray("10000000", "00001000", "00000001")
        val out = MathUtil.rotateBitsRight(in, 8)
        assert(out === byteArray("00000001", "10000000", "00001000"))
      }

      "works with 3 bytes with shift=9" in {
        val in = byteArray("10000000", "00001000", "00000001")
        val out = MathUtil.rotateBitsRight(in, 9)
        assert(out === byteArray("00000000", "11000000", "00000100"))
      }

      "works with 3 bytes with shift=16" in {
        val in = byteArray("10000000", "00001000", "00000001")
        val out = MathUtil.rotateBitsRight(in, 16)
        assert(out === byteArray("00001000", "00000001", "10000000"))
      }

      "works with 3 bytes with shift=23" in {
        val in = byteArray("10000000", "00001000", "00000001")
        val out = MathUtil.rotateBitsRight(in, 23)
        assert(out === byteArray("00000000", "00010000", "00000011"))
      }

      "multiple shifts produces the same input" in {
        val in = byteArray("10000000", "00001000", "00000001")
        val o1 = MathUtil.rotateBitsRight(in, 6)
        val o2 = MathUtil.rotateBitsRight(o1, 9)
        val o3 = MathUtil.rotateBitsRight(o2, 9)
        assert(in === o3)
      }
    }
  }

}

object MathUtilTest {

  @varargs
  def byteArray(xs: String*): Array[Byte] = {
    xs.map(_.b).toArray
  }
  implicit class StringUtils(val x: String) extends AnyVal {
    def b: Byte = java.lang.Integer.parseInt(x, 2).toByte
  }
}
