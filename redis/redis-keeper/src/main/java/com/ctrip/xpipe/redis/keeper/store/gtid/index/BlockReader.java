package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockReader implements AutoCloseable {

    private long startOffset;
    private long endOffset;
    private com.ctrip.xpipe.api.utils.ControllableFile controllableFile;

    public BlockReader(long startOffset, long endOffset, File file) throws IOException {
        if (startOffset < 0 || endOffset < startOffset) {
            throw new IllegalArgumentException("Invalid start or end offset.");
        }
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.controllableFile = new com.ctrip.xpipe.utils.DefaultControllableFile(file);
    }

    public static long seek(AsyncFileSystem fs, AsyncFile blockFile, long startOffset, long endOffset, int arrayIndex)
            throws IOException {
        long size = endOffset - startOffset;
        ByteBuf buffer = AsyncFileSystemHelper.await(fs.read(blockFile, size, startOffset), "read block file");
        try {
            return VarInt.decodeArray(buffer.nioBuffer(), arrayIndex);
        } finally {
            buffer.release();
        }
    }

    private ByteBuffer readIndexFile() throws IOException {
        long size = endOffset - startOffset;
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
        this.controllableFile.getFileChannel().position(startOffset);
        int bytesRead = this.controllableFile.getFileChannel().read(buffer);
        if (bytesRead != size) {
            throw new IOException("Could not read the specified amount of data from the file.");
        }
        buffer.flip();
        return buffer;
    }

    public long seek(int arrayIndex) throws IOException {
        ByteBuffer buffer = readIndexFile();
        return VarInt.decodeArray(buffer, arrayIndex);
    }

    @Override
    public void close() throws IOException {
        this.controllableFile.close();
    }
}
