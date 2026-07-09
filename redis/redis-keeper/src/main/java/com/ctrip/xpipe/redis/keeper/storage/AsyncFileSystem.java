package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import io.netty.buffer.ByteBuf;

// currently only support append only mode and atomic replace mode.
// All read/write operations attempt to read/write as much as possible until EOF, I/O error, or completion.
// also support synchronous operations. add on need
public interface AsyncFileSystem {
    void shutdown();

    // ---- AsyncFile ----
    // lenient: if true and path is not a regular file, I/O operations will throw NPE
    CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant);
    // Open the file synchronously.
    AsyncFile openSync(String path, boolean write, boolean atomicReplace, boolean lenient, String tenant);
    CompletableFuture<Boolean> isFile(AsyncFile file);
    CompletableFuture<Boolean> isDirectory(String path);
    CompletableFuture<Long> lastModified(AsyncFile file);
    // Only available in read mode.
    CompletableFuture<Void> position(AsyncFile file, long position);
    // Caller must release() the returned ByteBuf when done.
    CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset);
    // Read the file synchronously. alignSize=0 means no alignment.
    // When alignSize > 0, the read range is expanded so both start and end are aligned to alignSize boundaries.
    // The returned buffer's readerIndex points to the requested offset (leading padding is skipped),
    // and total capacity covers the full aligned range, allowing zero-copy chunk slicing.
    // Caller must release() the returned ByteBuf when done.
    ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize);
    // Caller must release() the returned ByteBuf when done.
    CompletableFuture<ByteBuf> read(AsyncFile file, long length);
    CompletableFuture<Long> write(AsyncFile file, ByteBuf data);
    long writeSync(AsyncFile file, ByteBuf data);
    CompletableFuture<Void> delete(String path);
    void deleteSync(String path);
    CompletableFuture<Boolean> exists(String path);
    CompletableFuture<Long> size(AsyncFile file);
    long sizeSync(AsyncFile file);
    CompletableFuture<Boolean> mkdir(String path, boolean recursive);
    CompletableFuture<Boolean> rmdir(String path, boolean recursive);
    // Only available in write mode.
    CompletableFuture<Void> truncate(AsyncFile file, long size);
    void truncateSync(AsyncFile file, long size);
    CompletableFuture<Void> close(AsyncFile file);
    void closeSync(AsyncFile file);
    // Only available in write mode.
    CompletableFuture<Void> fsync(AsyncFile file);
    void fsyncSync(AsyncFile file);
    // segment file return as raw file.
    CompletableFuture<List<String>> list(String path);
    CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target);

    // ---- AsyncSegmentFile ----
    // A segment file represents a list of files with the same prefix and monotonically increasing offsets.
    // Each segment file has 0 to n index files which share the same lifecycle.
    // Segment files operate using logical offsets instead of physical offsets.
    // It automatically deletes preceding non-contiguous segment files.
    // Write mode always opens the latest segment file and index files, and auto-closes them upon rollover.
    // Read mode opens the segment file after the first read, and opens index files when getCurrentIndexFiles() is called. They auto-close when reading the next segment.
    // For a newly created (empty) segment file in write mode, the segment file will start at 0 unless truncate(offset) is called to specify the initial offset before writing.
    CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant);
    AsyncSegmentFile openSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant);
    // Only available in read mode.
    // Mixing them with pread/transferTo on the same AsyncSegmentFile is NOT supported
    // as pread/transferTo also require a certain segment to be opened.
    CompletableFuture<Void> position(AsyncSegmentFile file, long offset);
    // Mixing them with pread/transferTo on the same AsyncSegmentFile is NOT supported
    // as pread/transferTo also require a certain segment to be opened.
    // Caller must release() the returned ByteBuf when done.
    CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length);
    // Mixing this with read()/position()/transferTo on the same AsyncSegmentFile is NOT supported
    // as it also requires a certain segment to be opened.
    // Caller must release() the returned ByteBuf when done.
    CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset);
    CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data);
    // Only available in write mode.
    CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file);
    List<Long> list(AsyncSegmentFile file);
    // Returns the start offset of the segment associated with the current position.
    // Returns -1 if the position is invalid.
    long getCurrentSegmentStartOffset(AsyncSegmentFile file);
    // position to one segment and then call this method to get the current index files.
    CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes);
    CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file);
    CompletableFuture<Long> size(AsyncSegmentFile file);
    CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset);
    CompletableFuture<Long> lastModified(AsyncSegmentFile file);
    CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset);
    // startOffsets must be ordered and contiguous from the first offset, will delete segments accordingly.
    // Cannot delete the last segment.
    // Only available in write mode.
    CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets);
    // Delete all known segment and index files. Caller must close() all open file objects (including this one) to release resources.
    // Only available in write mode.
    CompletableFuture<Void> delete(AsyncSegmentFile file);
    // Truncate at logical offset, returning the index files of the resulting segment.
    // If offset is in [minOffset, maxOffset + lastSegmentSize], truncate the containing segment and delete those to its right;
    // otherwise delete everything and create a new empty segment starting at offset.
    // Index file contents are NOT truncated; the caller must adjust them.
    // Only available in write mode.
    CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset);
    CompletableFuture<Void> close(AsyncSegmentFile file);
    void closeSync(AsyncSegmentFile file);
    // Only available in write mode.
    CompletableFuture<Void> fsync(AsyncSegmentFile file);
    // Mixing transferTo with read()/position()/pread on the same AsyncSegmentFile is NOT supported
    // as it also requires a certain segment to be opened.
    CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target);
}
