package com.worksap.nlp.uzushio.lib.stats;

public class NgramBitSignatures {

    public final static long HASH_SEED = 15213125612L;
    public final static long HASH_MULT = 6364136223846793005L;
    public final static long HASH_ADD = 1442695040888963407L;

    public static final int BITS_IN_LONG = 64;
    public static final int BIT_MASK = BITS_IN_LONG - 1;


    public static long mix(long state, long value) {
        long x = (value + HASH_ADD) ^ state;
        return Long.rotateLeft(x * HASH_MULT, 23);
    }

    public static long mix(long state, char value) {
        return mix(state, value & 0xffffL);
    }

    public static class UnigramUpTo16Chars {
        public final static int SIG_LEN = 4; // 256 bits for unigrams
        final static int BYTE_MASK = (SIG_LEN * BITS_IN_LONG - 1) ^ BIT_MASK;
        static long[] compute(CharSequence data) {
            long[] bitset = new long[SIG_LEN];
            int len = data.length();
            for (int chidx = 0; chidx < len; ++chidx) {

                long hash1 = mix(HASH_SEED, data.charAt(chidx)); // unigram hash
                {
                    long hashVal = mix(hash1, 1L);
                    int idx = (int)((hashVal >>> 32) ^ hashVal);
                    int bitIdx = (idx & BIT_MASK);
                    int byteIdx = (idx & BYTE_MASK) >>> 6;
                    bitset[byteIdx] |= (1L << bitIdx);
                }

            }
            return bitset;
        }
    }

    public static class UnigramBigramMoreThan16Chars {

        public final static int SIG_LEN1 = UnigramUpTo16Chars.SIG_LEN;
        public final static int SIG_LEN2 = 8;
        final static int BYTE_MASK1 = (SIG_LEN1 * BITS_IN_LONG - 1) ^ BIT_MASK;
        final static int BYTE_MASK2 = (SIG_LEN2 * BITS_IN_LONG - 1) ^ BIT_MASK;

        static long[] compute(CharSequence data) {
            long[] bitset = new long[SIG_LEN1 + SIG_LEN2];
            int len = data.length();
            for (int chidx = 0; chidx < len; ++chidx) {

                long state = mix(HASH_SEED, data.charAt(chidx)); // unigram hash
                {
                    long hashVal = mix(state, 1L);
                    int idx = (int)((hashVal >>> 32) ^ hashVal);
                    int bitIdx = (idx & BIT_MASK);
                    int byteIdx = (idx & BYTE_MASK1) >>> 6;
                    bitset[byteIdx] |= (1L << bitIdx);
                }

                if (chidx + 1 >= len) continue;
                state = mix(state, data.charAt(chidx + 1)); // bigram hash
                {
                    long hashVal = mix(state, 1L);
                    int idx = (int)((hashVal >>> 32) ^ hashVal);
                    int bitIdx = (idx & BIT_MASK);
                    int byteIdx = ((idx & BYTE_MASK2) >>> 6) + SIG_LEN1;
                    bitset[byteIdx] |= (1L << bitIdx);
                }

            }
            return bitset;
        }
    }

    public static final int SHORT_STRING = 16;

    public static long[] computeShortSignature(CharSequence data) {
        if (data.length() <= SHORT_STRING) {
            return UnigramUpTo16Chars.compute(data);
        } else {
            return UnigramBigramMoreThan16Chars.compute(data);
        }
    }

    public static float computeSignatureOverlap(long[] s1, long[] s2) {
        int length = Integer.min(s1.length, s2.length);
        int intersectionBits = 0;
        int unionBits = 0;
        for (int i = 0; i < length; ++i) {
            long v1 = s1[i];
            long v2 = s2[i];
            intersectionBits += Long.bitCount(v1 & v2);
            unionBits += Long.bitCount(v1 | v2);
        }
        return (float) intersectionBits / (float) unionBits;
    }

}
