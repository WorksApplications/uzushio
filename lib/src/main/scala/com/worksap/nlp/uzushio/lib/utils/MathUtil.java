package com.worksap.nlp.uzushio.lib.utils;

import it.unimi.dsi.fastutil.floats.FloatArrays;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class MathUtil {

    private MathUtil() {
        // static only class, no instances
    }

    public static void addArray(long[] result, long[] source) {
        if (result.length != source.length) {
            // enforce compiler invariant on length to remove bounds checks from the loop
            throw new IllegalArgumentException("source.length != result.length");
        }
        int len = result.length;
        for (int i = 0; i < len; ++i) {
            result[i] += source[i];
        }
    }

    public static long longFromBytes(byte[] data, int offset) {
        int len = data.length;
        if (offset + 8 <= len) {
            return ((data[offset + 0] & 0xffL) << 0) | ((data[offset + 1] & 0xffL) << 8) |
                    ((data[offset + 2] & 0xffL) << 16) | ((data[offset + 3] & 0xffL) << 24) |
                    ((data[offset + 4] & 0xffL) << 32) | ((data[offset + 5] & 0xffL) << 40) |
                    ((data[offset + 6] & 0xffL) << 48) | ((data[offset + 7] & 0xffL) << 56);
        } else {
            long result = 0L;
            for (int i = 0; i + offset < len; ++i) {
                result |= ((data[offset + i] & 0xffL) << i * 8);
            }
            return result;
        }
    }

    /**
     * This function rotates bits of the passed byte array to the right, as if the passed byte array was a single long
     * unsigned integer.
     *
     * @param data
     *         input bytes
     * @param shift
     *         step of the rotation, must be {@code >= 0} and {@code < data.length * 8}, otherwise UB
     * @return new byte array with rotated bits or the passed array if shift == 0
     */
    @NotNull
    public static byte[] rotateBitsRight(@NotNull byte[] data, int shift) {
        if (shift < 0) {
            throw new IllegalArgumentException("shift < 0");
        }
        if (shift == 0) {
            return data;
        }
        int length = data.length;
        byte[] result = new byte[length];

        int start = shift / 8;
        int lshift = shift % 8;
        if (lshift == 0) {
            System.arraycopy(data, length - start, result, 0, start);
            System.arraycopy(data, 0, result, start, length - start);
        } else {
            start = -start;
            if (start < 0) {
                start += length;
            }
            int prevIdx = start - 1;
            if (prevIdx < 0) {
                prevIdx += length;
            }
            byte p1 = data[prevIdx];
            int rshift = 8 - lshift;
            for (int i = 0; i < length; ++i) {
                int j = start + i;
                if (j >= length) {
                    j -= length;
                }
                byte p2 = data[j];
                byte b = (byte) (((p2 & 0xff) >>> lshift) | ((p1 << rshift) & 0xff));
                result[i] = b;
                p1 = p2;
            }
        }

        return result;
    }

    // compute number of bits which are matching in both numbers (both 1s and 0s)
    public static int matchingBits(long x, long y) {
        return Long.bitCount(~(x ^ y));
    }

    public static float ratio(int count, int total) {
        if (count == 0 || total == 0) {
            return 0.0f;
        }
        return (float) count / (float) total;
    }

    private static final double DOUBLE_UNIT = 1.0 / (1L << 54);
    private static final long LONG_DOUBLE_MASK = (1L << 54) - 1L; // 53 bits
    public static double asRandomDouble(long seed) {
        long masked = seed & LONG_DOUBLE_MASK;
        return masked * DOUBLE_UNIT;
    }
 }
