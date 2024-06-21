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

import java.nio.ByteBuffer;

final class BufferBitSink implements BitSink {
    private static final int FIRST_BIT = 0x80;
    private static final int LAST_BIT = 0x1;

    private final ByteBuffer dest;

    private int mask;

    public BufferBitSink(ByteBuffer dest) {
        this.dest = dest;
        mask = FIRST_BIT;
    }

    @Override
    public void write(boolean bit) {
        if (mask == LAST_BIT) {
            if (bit) {
                dest.put((byte) ((dest.get(dest.position()) & 0xFF) | mask));
            } else {
                dest.position(dest.position() + 1);
            }
            mask = FIRST_BIT;
        } else {
            if (mask == FIRST_BIT) {
                dest.put(dest.position(), (byte) (bit ? FIRST_BIT : 0));
            } else if (bit) {
                dest.put(dest.position(),
                        (byte) ((dest.get(dest.position()) & 0xFF) | mask));
            }
            mask >>= 1;
        }
    }
}
