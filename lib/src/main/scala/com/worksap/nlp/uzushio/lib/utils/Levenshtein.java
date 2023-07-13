package com.worksap.nlp.uzushio.lib.utils;

public class Levenshtein {

    private Levenshtein() {
        // instances forbidden
    }

    public static int[] floatRange(int len) {
        int[] result = new int[len];
        for (int i = 0; i < len; ++i) {
            result[i] = i * 100;
        }
        return result;
    }

    public static int levenshteinDistance(CharSequence a, CharSequence b, int limit, int step) {
        int[] row0 = floatRange(b.length() + 1);
        int[] row1 = new int[b.length() + 1];

        int al = a.length();
        int bl = b.length();
        for (int i = 0; i < al; ++i) {
            char c = a.charAt(i);
            for (int j = 1; j < bl; ++j) {
                char x = b.charAt(j - 1);

            }
        }
        return -1;
    }

    private static final int UMASK = 0x7fff_ffff;
    private static final int FMASK = 0x8000_0000;

    public static int levStep(int compressedScore, int scoreA, int scoreB) {
        int uscore = compressedScore & UMASK;
        int flag = compressedScore & FMASK;

        int score = scoreA;
        if (flag != 0) {
            score = scoreB;
        }

        return (uscore + score) & flag;
    }

    public static int levStepB(int compressedScore, int scoreA, int scoreB) {
        int uscore = compressedScore & UMASK;
        int flag = compressedScore & FMASK;

        int score = scoreA;
        if (flag != 0) {
            score = scoreB;
        }

        return (uscore + score) & flag;
    }

    public static final int MASK1 = 0b11111111;
    public static final int MASK2 = 0b00000000;
}
