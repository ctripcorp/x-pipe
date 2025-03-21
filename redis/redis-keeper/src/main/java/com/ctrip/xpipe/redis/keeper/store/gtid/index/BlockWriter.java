package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockWriter implements AutoCloseable {

    private int cmdOffset;
    private int size;
    private ControllableFile controllableFile;
    private String currentUuid;
    private long currentGno;

    public static final int BLOCK_NAX_SIZE = 8 * 1024;

    public BlockWriter(String currentUuid, long gno, int cmdOffset, String file) throws IOException {
        this.cmdOffset = cmdOffset;
        this.currentUuid = currentUuid;
        this.currentGno = gno;
        this.size = 0;
        this.controllableFile = new DefaultControllableFile(file);
    }

    public void recover(long offset, long startGno) throws IOException {
        long fileSize = this.controllableFile.getFileChannel().size();
        if (offset < 0 || offset >= fileSize) {
            throw new IOException("Invalid offset for recovery.");
        }

        long remain = this.controllableFile.getFileChannel().size() - offset;
        if(remain >= BLOCK_NAX_SIZE * Integer.BYTES) {
            // remain
            throw new IOException(String.format("[%d] recover offset %d fail", remain, offset));
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) remain);
        this.controllableFile.getFileChannel().position(offset);
        int bytesRead = this.controllableFile.getFileChannel().read(buffer);
        if (bytesRead != remain) {
            throw new IOException("Could not read the specified amount of data from the file.");
        }
        buffer.flip(); // Prepare the buffer for reading
        int cnt = 0;
        long gno = startGno - 1;
        while (buffer.remaining() > 0) {
            int val = VarInt.getVarInt(buffer);
            this.cmdOffset += val;
            cnt++;
            gno++;
        }
        this.size = cnt;
        this.currentGno = gno;
    }

    private boolean isFull() {
        return this.size >= BLOCK_NAX_SIZE;
    }

    private boolean isGnoGap(String uuid, long gno) {
        return !StringUtil.trimEquals(this.currentUuid, uuid) || gno != currentGno + 1;
    }

    public boolean needChangeBlock(String uuid, long gno) {
        return isFull() || isGnoGap(uuid, gno);
    }

    public void append(String uuid, long gno, int offset) throws IOException {
         int val = offset - cmdOffset;
         ByteBuffer byteBuffer =  VarInt.encodeToByteBuffer(val);

         int bytesWritten = this.controllableFile.getFileChannel().write(byteBuffer);
         if (bytesWritten != byteBuffer.limit()) {
            throw new IOException("Failed to write the complete data to the file.");
         }

         cmdOffset = offset;
         this.currentGno = gno;
         this.currentUuid = uuid;
         this.size++;
    }

    public long getPosition() throws IOException {
        return this.controllableFile.getFileChannel().position();
    }

    public void finishWriter() throws IOException {
        this.close();
    }

    public int getSize() {
        return this.size;
    }

    @Override
    public void close() throws IOException {
        this.controllableFile.close();
    }

}
