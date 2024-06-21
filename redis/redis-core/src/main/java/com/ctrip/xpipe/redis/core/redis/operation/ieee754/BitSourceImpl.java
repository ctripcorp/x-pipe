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

abstract class BitSourceImpl implements BitSource {
    private static final int FIRST_BIT = 0x80;
    private static final int LAST_BIT = 0x1;

    private int mask;
    private int current;

    public BitSourceImpl() {
        mask = FIRST_BIT;
    }

    @Override
    public final boolean next() {
        if (mask == FIRST_BIT) {
            current = nextByte() & 0xFF;
        }
        boolean r = (current & mask) != 0;
        if (mask == LAST_BIT) {
            mask = FIRST_BIT;
        } else {
            mask >>= 1;
        }
        return r;
    }

    protected abstract byte nextByte();

}
