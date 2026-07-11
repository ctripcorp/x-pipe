package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface AsyncCommandStore {

    int DEFAULT_ASYNC_WRITE_MAX_BYTES = 65536;

    AsyncFileSystem getAsyncFileSystem();

    AsyncSegmentFile getAsyncSegmentFile();

    /**
     * The unique write-mode {@link AsyncSegmentFile} owned by the CommandStore (spec §3.7.2/§3.7.3).
     * IndexStore uses this reference to call {@code fs.getCurrentIndexFiles(writeSeg, prefixes)};
     * ownership is NOT transferred — CmdStore alone drives roll/truncate on the segment.
     */
    AsyncSegmentFile getWriteSegmentFile();

    File getCommandBaseDir();

    String getCommandFileNamePrefix();

    List<String> getCommandIndexPrefixes();

    ReplId getFileSystemReplId();

    int getAsyncWriteMaxBytes();

    /**
     * Truncate the companion index/block files of the current write segment (spec §3.7.3).
     * <b>Index-only</b>: neither cmd segment position nor {@code fs.truncate(writeSeg,...)} is touched.
     * Callers pass the V1 ({@code index_}/{@code block_}) or V2 ({@code indexv2_}/{@code blockv2_}) prefix
     * pair matching the writer being recovered.
     *
     * @return refreshed {@code {indexPrefix→AsyncFile, blockPrefix→AsyncFile}} handles for the write segment.
     *         Block is truncated first; on index failure the block is rolled back to its pre-call size.
     */
    Map<String, AsyncFile> truncateIndex(String indexPrefix, String blockPrefix,
                                         long indexSize, long blockSize) throws IOException;

    /**
     * Truncate the cmd segment tail down to {@code cmdStartOffset} (spec §3.7.3). Companion index files are
     * <b>not</b> modified by this call — callers must follow up with {@link #truncateIndex} when rolling back
     * a partially-indexed transaction.
     *
     * @return the index handles returned by {@code fs.truncate(writeSeg, cmdStartOffset)} for the resulting segment.
     */
    Map<String, AsyncFile> truncateCmdSegment(long cmdStartOffset) throws IOException;
}
