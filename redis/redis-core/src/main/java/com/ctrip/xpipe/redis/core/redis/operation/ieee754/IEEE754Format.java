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

public class IEEE754Format {
    /**
     * <dl>
     * <dt>IEEE 754 name</dt><dd>binary16</dd>
     * <dt>Sign bit</dt><dd>1 bit</dd>
     * <dt>Exponent width</dt><dd>5 bits</dd>
     * <dt>Significand precision</dt><dd>11 bits (10 explicitly stored)</dd>
     * </dl>
     */
    public static final IEEE754Format HALF = new IEEE754Format(5, 10, 15);

    /**
     * <dl>
     * <dt>IEEE 754 name</dt><dd>binary32</dd>
     * <dt>Sign bit</dt><dd>1 bit</dd>
     * <dt>Exponent width</dt><dd>8 bits</dd>
     * <dt>Significand precision</dt><dd>24 bits (23 explicitly stored)</dd>
     * </dl>
     */
    public static final IEEE754Format SINGLE = new IEEE754Format(8, 23, 127);

    /**
     * <dl>
     * <dt>IEEE 754 name</dt><dd>binary64</dd>
     * <dt>Sign bit</dt><dd>1 bit</dd>
     * <dt>Exponent width</dt><dd>11 bits</dd>
     * <dt>Significand precision</dt><dd>53 bits (52 explicitly stored)</dd>
     * </dl>
     */
    public static final IEEE754Format DOUBLE = new IEEE754Format(11, 52, 1023);

    /**
     * <dl>
     * <dt>IEEE 754 name</dt><dd>binary128</dd>
     * <dt>Sign bit</dt><dd>1 bit</dd>
     * <dt>Exponent width</dt><dd>15 bits</dd>
     * <dt>Significand precision</dt><dd>113 bits (112 explicitly stored)</dd>
     * </dl>
     */
    public static final IEEE754Format QUADRUPLE = new IEEE754Format(15, 64, 16383);

    /**
     * <dl>
     * <dt>IEEE 754 name</dt><dd>binary256</dd>
     * <dt>Sign bit</dt><dd>1 bit</dd>
     * <dt>Exponent width</dt><dd>19 bits</dd>
     * <dt>Significand precision</dt><dd>237 bits (236 explicitly stored)</dd>
     * </dl>
     */
    public static final IEEE754Format OCTUPLE = new IEEE754Format(19, 236, 262143);

    private final int exponentLength;
    private final int mantissaLength;
    private final BigInteger exponentBias;

    private IEEE754Format(
            int exponentLength,
            int mantissaLength,
            int exponentBias) {
        this.exponentLength = exponentLength;
        this.mantissaLength = mantissaLength;
        this.exponentBias = BigInteger.valueOf(exponentBias);
    }

    public int getExponentLength() {
        return exponentLength;
    }

    public int getMantissaLength() {
        return mantissaLength;
    }

    public BigInteger getExponentBias() {
        return exponentBias;
    }
}
