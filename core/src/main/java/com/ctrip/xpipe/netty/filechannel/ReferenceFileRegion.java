package com.ctrip.xpipe.netty.filechannel;

import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public interface ReferenceFileRegion extends FileRegion {

    ReferenceFileRegion EOF = new EOFFileRegion();

    void deallocate();

    long getTotalPos();

    void setTotalPos(long totalPos);

    class EOFFileRegion implements ReferenceFileRegion {

        private EOFFileRegion() {

        }

        @Override
        public long getTotalPos() {
            return 0;
        }

        @Override
        public void setTotalPos(long totalPos) {

        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public long transfered() {
            return 0;
        }

        @Override
        public long transferred() {
            return 0;
        }

        @Override
        public long count() {
            return 0;
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            return 0;
        }

        @Override
        public FileRegion retain() {
            return null;
        }

        @Override
        public FileRegion retain(int increment) {
            return null;
        }

        @Override
        public FileRegion touch() {
            return null;
        }

        @Override
        public FileRegion touch(Object hint) {
            return null;
        }

        @Override
        public int refCnt() {
            return 0;
        }

        @Override
        public boolean release() {
            return false;
        }

        @Override
        public boolean release(int decrement) {
            return false;
        }

        @Override
        public void deallocate() {

        }
    }

}
