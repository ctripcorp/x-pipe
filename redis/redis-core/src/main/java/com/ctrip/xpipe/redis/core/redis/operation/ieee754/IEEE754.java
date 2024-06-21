/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Glenn Lane
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.ctrip.xpipe.redis.core.redis.operation.ieee754;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

public abstract class IEEE754 extends Number {
    private static final long serialVersionUID = 3364176842115330372L;

    private static final class Infinity extends IEEE754 {
        private static final long serialVersionUID = -2201241195278964739L;

        private final boolean negative;

        public Infinity(boolean negative) {
            this.negative = negative;
        }

        @Override
        public void toBits(IEEE754Format format, BitSink out) {
            out.write(negative);
            for (int i = 0; i < format.getExponentLength(); i++) {
                out.write(true);
            }
            for (int i = 0; i < format.getMantissaLength(); i++) {
                out.write(false);
            }
        }

        @Override
        public String toString() {
            return Double.toString(doubleValue());
        }

        @Override
        public double doubleValue() {
            return negative ?
                    Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
    }

    private static final class Zero extends IEEE754 {
        private static final long serialVersionUID = -1813412510288843500L;

        private final boolean negative;

        public Zero(boolean negative) {
            this.negative = negative;
        }

        @Override
        public void toBits(IEEE754Format format, BitSink out) {
            out.write(negative);
            for (int i = 0; i < format.getExponentLength(); i++) {
                out.write(false);
            }
            for (int i = 0; i < format.getMantissaLength(); i++) {
                out.write(false);
            }
        }

        @Override
        public String toString() {
            if (negative) {
                return "-0 x 2^0";
            }
            return "0 x 2^0";
        }

        @Override
        public double doubleValue() {
            return negative ? -0D : 0D;
        }
    }

    public static final IEEE754 POSITIVE_ZERO = new Zero(false);
    public static final IEEE754 NEGATIVE_ZERO = new Zero(true);

    public static final IEEE754 POSITIVE_INFINITY = new Infinity(false);
    public static final IEEE754 NEGATIVE_INFINITY = new Infinity(true);

    public static final IEEE754 NaN = new IEEE754() {
        private static final long serialVersionUID = 4967162119419949857L;

        @Override
        public void toBits(IEEE754Format format, BitSink out) {
            out.write(false);
            for (int i = 0; i < format.getExponentLength(); i++) {
                out.write(true);
            }
            out.write(true);
            for (int i = 1; i < format.getMantissaLength(); i++) {
                out.write(false);
            }
        }

        @Override
        public String toString() {
            return Double.toString(Double.NaN);
        }

        @Override
        public double doubleValue() {
            return Double.NaN;
        }
    };

    public static final class IEEE754Number extends IEEE754 {
        private static final long serialVersionUID = -5536123689937274755L;

        private static final int HC_PRIME = 31;

        private final BigInteger exponent;
        private final BigInteger significand;

        public IEEE754Number(
                BigInteger exponent,
                BigInteger significand) {
            if (exponent == null) {
                throw new NullPointerException();
            }
            this.exponent = exponent;
            if (significand == null) {
                throw new NullPointerException();
            }
            this.significand = significand;
        }

        public BigInteger getExponent() {
            return exponent;
        }

        public BigInteger getSignificand() {
            return significand;
        }

        private static BigInteger roundingShiftRight(BigInteger bi, int n) {
            int bitIndex = n - 1;
            if (bi.testBit(bitIndex)) {
                bitIndex++;
                while (bi.testBit(bitIndex)) {
                    bi = bi.clearBit(bitIndex);
                    bitIndex++;
                }
                bi = bi.setBit(bitIndex);
            }
            return bi.shiftRight(n);
        }

        private boolean writeSubNormalOrZero(
                boolean negative,
                BigInteger mantissaBits,
                IEEE754Format format,
                BitSink out) {
            BigInteger exponentBits = exponent
                    .add(format.getExponentBias())
                    .add(BigInteger.valueOf(mantissaBits.bitLength()));
            BigInteger rightShift =
                    BigInteger.ONE.subtract(exponentBits);
            if (rightShift.signum() == -1) {
                return false;
            }
            if (rightShift.compareTo(
                    BigInteger.valueOf(format.getMantissaLength())) > 0) {
                /*
                 * The entire mantissa will be zeros.  We don't use >= 0 because
                 * we might still get one bit after right-shifting the entire
                 * mantissa, due to rounding.  Any more than that though: it's
                 * impossible to have a non-zero mantissa.
                 */
                (negative ? NEGATIVE_ZERO : POSITIVE_ZERO).toBits(format, out);
                return true;
            }
            int zeroPadCount = rightShift.intValue();
            int sigBitCount = format.getMantissaLength() - zeroPadCount;
            if (mantissaBits.bitLength() > sigBitCount) {
                mantissaBits = roundingShiftRight(mantissaBits,
                        mantissaBits.bitLength() - sigBitCount);
                if (mantissaBits.signum() == 0) {
                    (negative ? NEGATIVE_ZERO : POSITIVE_ZERO).toBits(
                            format, out);
                    return true;
                }
                if (mantissaBits.bitLength() != sigBitCount) {
                    /*
                     * The significant bits from the mantissa must have all been
                     * set, so the rounding right-shift gave us a mantissa whose
                     * length exceeds the significant bit-count (by one).  We
                     * can accommodate this by taking away a left-padding zero
                     */
                    assert mantissaBits.bitLength() == sigBitCount + 1;
                    if (zeroPadCount == 0) {
                        /*
                         * We were already against the decimal point.  After
                         * rounding, this number is too large to be expressed as
                         * a sub-normal
                         */
                        return false;
                    }
                    zeroPadCount--;
                }
            }

            out.write(negative);
            for (int i = 0; i < format.getExponentLength(); i++) {
                out.write(false);
            }
            int i;
            for (i = 0; i < zeroPadCount; i++) {
                out.write(false);
            }
            int mantissaBitIndex = mantissaBits.bitLength() - 1;
            for (; i < format.getMantissaLength() && mantissaBitIndex >= 0; i++) {
                out.write(mantissaBits.testBit(mantissaBitIndex));
                mantissaBitIndex--;
            }
            for (; i < format.getMantissaLength(); i++) {
                out.write(false);
            }
            return true;
        }

        private boolean writeNormalOrInfinity(
                boolean negative,
                BigInteger mantissaBits,
                IEEE754Format format,
                BitSink out) {
            final int sigBitCount = format.getMantissaLength() + 1;
            BigInteger roundedMantissaBits;
            int shift = mantissaBits.bitLength() - sigBitCount;
            if (shift > 0) {
                roundedMantissaBits = roundingShiftRight(mantissaBits, shift);
                if (roundedMantissaBits.bitLength() != sigBitCount) {
                    assert roundedMantissaBits.bitLength() == sigBitCount + 1;
                    shift++;
                    roundedMantissaBits = roundingShiftRight(
                            mantissaBits, shift);
                    assert roundedMantissaBits.bitLength() == sigBitCount;
                }
            } else {
                roundedMantissaBits = mantissaBits;
                shift = 0;
            }
            BigInteger exponentBits = exponent
                    .add(BigInteger.valueOf(roundedMantissaBits.bitLength() - 1))
                    .add(format.getExponentBias())
                    .add(BigInteger.valueOf(shift));

            if (exponentBits.signum() != 1) {
                /*
                 * Exponent is too small
                 */
                return false;
            }

            /*
             * The exponent bits are a positive non-zero number
             */
            if (exponentBits.bitLength() > format.getExponentLength()
                    || exponentBits.bitCount() == format.getExponentLength()) {
                /*
                 * The amount of exponent bits required exceeds the
                 * format's exponent length, or requires exactly the
                 * format's exponent length, but with all bits set (a
                 * condition that is special, used to express infinity).
                 *
                 * In either case, we write infinity
                 */
                (negative ? NEGATIVE_INFINITY : POSITIVE_INFINITY)
                        .toBits(format, out);
            } else {
                out.write(negative);

                /*
                 * The amount of exponent bits required will fit in the
                 * format's exponent length
                 */
                for (int i = format.getExponentLength() - 1; i >= 0; i--) {
                    out.write(exponentBits.testBit(i));
                }

                /*
                 * The first set mantissa bit is implied: it isn't written
                 */
                int mantissaBitIndex = roundedMantissaBits.bitLength() - 2;
                int i;
                for (i = 0; i < format.getMantissaLength()
                        && mantissaBitIndex >= 0; i++) {
                    out.write(roundedMantissaBits.testBit(mantissaBitIndex));
                    mantissaBitIndex--;
                }

                /*
                 * The mantissa may not have been large enough to fill the
                 * format's mantissa length.  Write the remaining zero bits.
                 */
                for (; i < format.getMantissaLength(); i++) {
                    out.write(false);
                }
            }
            return true;
        }

        @Override
        public void toBits(IEEE754Format format, BitSink out) {
            final boolean negative;
            BigInteger mantissaBits;
            if (this.significand.signum() == -1) {
                negative = true;
                mantissaBits = significand.negate();
            } else {
                negative = false;
                mantissaBits = significand;
            }

            if (!writeNormalOrInfinity(negative, mantissaBits, format, out)) {
                writeSubNormalOrZero(negative, mantissaBits, format, out);
            }
        }

        @Override
        public int hashCode() {
            int hc = HC_PRIME;
            hc = hc * HC_PRIME + exponent.hashCode();
            hc = hc * HC_PRIME + significand.hashCode();
            return hc;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof IEEE754Number)) {
                return false;
            }
            IEEE754Number other = (IEEE754Number) obj;
            return exponent.equals(other.exponent)
                    && significand.equals(other.significand);
        }

        @Override
        public String toString() {
            return significand + " x 2^" + exponent;
        }

        @Override
        public double doubleValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(8);
            DoubleBuffer db = buf.asDoubleBuffer();
            toBits(IEEE754Format.DOUBLE, BitUtils.wrapSink(buf));
            return db.get(0);
        }
    }

    private IEEE754() {
    }

    public abstract void toBits(IEEE754Format format, BitSink out);

    public static IEEE754 valueOf(double value) {
        ByteBuffer buf = ByteBuffer.allocateDirect(8);
        buf.asDoubleBuffer().put(value);
        return decode(IEEE754Format.DOUBLE, BitUtils.wrapSource(buf));
    }

    public static IEEE754 valueOf(float value) {
        ByteBuffer buf = ByteBuffer.allocateDirect(4);
        buf.asFloatBuffer().put(value);
        return decode(IEEE754Format.SINGLE, BitUtils.wrapSource(buf));
    }

    public static IEEE754 decode(IEEE754Format format, BitSource in) {
        final boolean negative = in.next();
        BigInteger exponentBits = BigInteger.ZERO;
        for (int i = format.getExponentLength() - 1; i >= 0; i--) {
            if (in.next()) {
                exponentBits = exponentBits.setBit(i);
            }
        }

        /*
         * Check for NaN or infinity
         */
        if (exponentBits.bitCount() == format.getExponentLength()) {
            boolean nan = false;
            int i;
            for (i = 0; i < format.getMantissaLength(); i++) {
                /*
                 * No break here: we should consume all mantissa bits even if we
                 * discover NaN before reading the last mantissa bit
                 */
                nan |= in.next();
            }
            return nan ? NaN : negative ?
                    NEGATIVE_INFINITY : POSITIVE_INFINITY;
        }

        /*
         * Store the mantissa
         */
        BigInteger mantissaBits = BigInteger.ZERO;
        for (int i = format.getMantissaLength() - 1; i >= 0; i--) {
            if (in.next()) {
                mantissaBits = mantissaBits.setBit(i);
            }
        }

        /*
         * Check for zero or subnormal
         */
        if (exponentBits.equals(BigInteger.ZERO)) {
            if (mantissaBits.equals(BigInteger.ZERO)) {
                /*
                 * Zero
                 */
                return negative ? NEGATIVE_ZERO : POSITIVE_ZERO;
            }

            /*
             * Subnormal
             */
            int leftBias = format.getMantissaLength()
                    - mantissaBits.bitLength();
            mantissaBits = mantissaBits.shiftRight(
                    mantissaBits.getLowestSetBit());
            return new IEEE754Number(
                    BigInteger.ONE
                            .subtract(format.getExponentBias())
                            .subtract(BigInteger.valueOf(
                                    mantissaBits.bitLength()))
                            .subtract(BigInteger.valueOf(leftBias)),
                    negative ? mantissaBits.negate() : mantissaBits);
        }

        /*
         * Normal
         */
        //mantissaBits = mantissaBits.setBit(format.getMantissaLength());
        mantissaBits = mantissaBits.shiftRight(mantissaBits.getLowestSetBit());

        return new IEEE754Number(
                exponentBits
                        .subtract(format.getExponentBias())
                        .subtract(BigInteger.valueOf(
                                mantissaBits.bitLength() - 1)),
                negative ? mantissaBits.negate() : mantissaBits);
    }

    @Override
    public final float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public final int intValue() {
        return (int) doubleValue();
    }

    @Override
    public final long longValue() {
        return (long) doubleValue();
    }
}
