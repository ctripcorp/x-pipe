package com.ctrip.xpipe.utils;

/**
 * High-performance numeric parsing from byte arrays (digit characters only).
 * No intermediate String allocation.
 */
public class ByteUtil {

    private static final long LONG_MAX_DIV_10 = Long.MAX_VALUE / 10;

    /**
     * Parses a long from bytes[start..end) (end exclusive).
     *
     * @param bytes  digit characters '0'-'9', optionally one leading '-' when signed is true
     * @param start  start index (inclusive)
     * @param end    end index (exclusive)
     * @param signed if true, a leading '-' makes the result negative
     * @return parsed long value
     * @throws NumberFormatException if format is invalid or overflow
     */
    public static long parseLong(byte[] bytes, int start, int end, boolean signed) {
        if (bytes == null || start < 0 || end > bytes.length || start >= end) {
            throw new IllegalArgumentException("invalid range: start=" + start + ", end=" + end + ", length=" + (bytes == null ? "null" : bytes.length));
        }
        final int originalStart = start;
        final int originalEnd = end;
        int i = start;
        boolean negative = false;
        if (signed && bytes[i] == '-') {
            negative = true;
            i++;
            if (i >= end) {
                throw forInputString(bytes, originalStart, originalEnd);
            }
        }
        long result = 0;
        for (; i < end; i++) {
            byte b = bytes[i];
            if (b < '0' || b > '9') {
                throw forInputString(bytes, originalStart, originalEnd);
            }
            int digit = b - '0';
            int maxDigit = (negative && result == LONG_MAX_DIV_10) ? 8 : 7; // Long.MIN_VALUE ends with 8
            if (result > LONG_MAX_DIV_10 || (result == LONG_MAX_DIV_10 && digit > maxDigit)) {
                throw forInputString(bytes, originalStart, originalEnd);
            }
            result = result * 10 + digit;
        }
        return negative ? -result : result;
    }

    /**
     * Parses an int from bytes[start..end) (end exclusive).
     *
     * @param bytes  digit characters '0'-'9', optionally one leading '-' when signed is true
     * @param start  start index (inclusive)
     * @param end    end index (exclusive)
     * @param signed if true, a leading '-' makes the result negative
     * @return parsed int value
     * @throws NumberFormatException if format is invalid or overflow
     */
    public static int parseInt(byte[] bytes, int start, int end, boolean signed) {
        if (bytes == null || start < 0 || end > bytes.length || start >= end) {
            throw new IllegalArgumentException("invalid range: start=" + start + ", end=" + end + ", length=" + (bytes == null ? "null" : bytes.length));
        }
        final int originalStart = start;
        final int originalEnd = end;
        int i = start;
        boolean negative = false;
        if (signed && bytes[i] == '-') {
            negative = true;
            i++;
            if (i >= end) {
                throw forInputString(bytes, originalStart, originalEnd);
            }
        }
        // use long for magnitude so that Integer.MIN_VALUE (-2147483648) can be represented
        long result = 0;
        for (; i < end; i++) {
            byte b = bytes[i];
            if (b < '0' || b > '9') {
                throw forInputString(bytes, originalStart, originalEnd);
            }
            int digit = b - '0';
            if (result > LONG_MAX_DIV_10 || (result == LONG_MAX_DIV_10 && digit > Long.MAX_VALUE % 10)) {
                throw forInputString(bytes, originalStart, originalEnd);
            }
            result = result * 10 + digit;
        }
        if (negative) {
            result = -result;
            if (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE) {
                throw forInputString(bytes, originalStart, originalEnd);
            }
        } else {
            if (result > Integer.MAX_VALUE) {
                throw forInputString(bytes, originalStart, originalEnd);
            }
        }
        return (int) result;
    }

    private static NumberFormatException forInputString(byte[] bytes, int start, int end) {
        return new NumberFormatException("For input string: \"" + new String(bytes, start, end - start) + "\"");
    }
}
