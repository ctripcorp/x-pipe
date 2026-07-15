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
    default CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        throw new UnsupportedOperationException();
    }
    // Open the file synchronously.
    default AsyncFile openSync(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        throw new UnsupportedOperationException();
    }
    CompletableFuture<Boolean> isFile(AsyncFile file);
    CompletableFuture<Boolean> isDirectory(String path);
    CompletableFuture<Long> lastModified(AsyncFile file);
    // Only available in read mode.
    default CompletableFuture<Void> position(AsyncFile file, long position) {
        throw new UnsupportedOperationException();
    }
    default void positionSync(AsyncFile file, long position) {
        throw new UnsupportedOperationException();
    }
    // Caller must release() the returned ByteBuf when done.
    default CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        throw new UnsupportedOperationException();
    }
    // Read the file synchronously. alignSize=0 means no alignment.
    // When alignSize > 0, the read range is expanded so both start and end are aligned to alignSize boundaries.
    // The returned buffer's readerIndex points to the requested offset (leading padding is skipped),
    // and total capacity covers the full aligned range, allowing zero-copy chunk slicing.
    // Caller must release() the returned ByteBuf when done.
    default ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize) {
        throw new UnsupportedOperationException();
    }
    // Caller must release() the returned ByteBuf when done.
    default CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Long> write(AsyncFile file, ByteBuf data) {
        throw new UnsupportedOperationException();
    }
    default long writeSync(AsyncFile file, ByteBuf data) {
        throw new UnsupportedOperationException();
    }
    CompletableFuture<Void> delete(String path);
    default void deleteSync(String path) {
        throw new UnsupportedOperationException();
    }
    CompletableFuture<Boolean> exists(String path);
    default CompletableFuture<Long> size(AsyncFile file) {
        throw new UnsupportedOperationException();
    }
    default long sizeSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }
    CompletableFuture<Boolean> mkdir(String path, boolean recursive);
    CompletableFuture<Boolean> rmdir(String path, boolean recursive);
    // Only available in write mode.
    default CompletableFuture<Void> truncate(AsyncFile file, long size) {
        throw new UnsupportedOperationException();
    }
    default void truncateSync(AsyncFile file, long size) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Void> close(AsyncFile file) {
        throw new UnsupportedOperationException();
    }
    default void closeSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }
    // Only available in write mode.
    default CompletableFuture<Void> fsync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }
    default void fsyncSync(AsyncFile file) {
        throw new UnsupportedOperationException();
    }
    // segment file return as raw file.
    CompletableFuture<List<String>> list(String path);
    default CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }
    default long transferToSync(AsyncFile file, long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }

    // ---- AsyncSegmentFile ----
    // A segment file represents a list of files with the same prefix and monotonically increasing offsets.
    // Each segment file has 0 to n index files which share the same lifecycle.
    // Segment files operate using logical offsets instead of physical offsets.
    // It automatically deletes preceding non-contiguous segment files.
    // Write mode always opens the latest segment file and index files, and auto-closes them upon rollover.
    // Read mode opens the segment data channel lazily on read()/pread()/transferTo() only.
    // file.position tracks the last logical read offset (initialized to the first segment start on open).
    // getCurrentIndexFiles() opens index files for the segment at file.position without opening the data channel.
    // Opened data and index channels auto-close when read()/transferTo() advances to the next segment.
    // Mixing read()/position() with pread()/transferTo() on the same AsyncSegmentFile is NOT supported.
    // For a newly created (empty) segment file in write mode, the segment file will start at 0 unless truncate(offset) is called to specify the initial offset before writing.
    default CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        throw new UnsupportedOperationException();
    }
    default AsyncSegmentFile openSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        throw new UnsupportedOperationException();
    }
    // Only available in read mode.
    default CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        throw new UnsupportedOperationException();
    }
    default void positionSync(AsyncSegmentFile file, long offset) {
        throw new UnsupportedOperationException();
    }
    // Caller must release() the returned ByteBuf when done.
    default CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length) {
        throw new UnsupportedOperationException();
    }
    default ByteBuf readSync(AsyncSegmentFile file, long length) {
        throw new UnsupportedOperationException();
    }
    // Caller must release() the returned ByteBuf when done.
    default CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset) {
        throw new UnsupportedOperationException();
    }
    default ByteBuf readSync(AsyncSegmentFile file, long length, long offset) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data) {
        throw new UnsupportedOperationException();
    }
    default long writeSync(AsyncSegmentFile file, ByteBuf data) {
        throw new UnsupportedOperationException();
    }
    // Only available in write mode.
    default CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default Map<String, AsyncFile> rollSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    List<Long> list(AsyncSegmentFile file);
    // Returns the start offset of the currently opened segment; if none is open, returns the segment start
    // at file.position when it equals a segment boundary (e.g. on first open). Returns -1 otherwise.
    long getCurrentSegmentStartOffset(AsyncSegmentFile file);
    // Returns index files for the segment at file.position.
    default CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes) {
        throw new UnsupportedOperationException();
    }
    default Map<String, AsyncFile> getCurrentIndexFilesSync(AsyncSegmentFile file, List<String> indexPrefixes) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default Map<String, AsyncFile> getCurrentIndexFilesSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Long> size(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default long sizeSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        throw new UnsupportedOperationException();
    }
    default long sizeOfSegmentSync(AsyncSegmentFile file, long startOffset) {
        throw new UnsupportedOperationException();
    }
    CompletableFuture<Long> lastModified(AsyncSegmentFile file);
    CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset);
    // startOffsets must be ordered and contiguous from the first offset, will delete segments accordingly.
    // Cannot delete the last segment.
    // Only available in write mode.
    default CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        throw new UnsupportedOperationException();
    }
    default void deleteSegmentsSync(AsyncSegmentFile file, List<Long> startOffsets) {
        throw new UnsupportedOperationException();
    }
    // Delete all known segment and index files. Caller must close() all open file objects (including this one) to release resources.
    // Only available in write mode.
    CompletableFuture<Void> delete(AsyncSegmentFile file);
    // Truncate at logical offset, returning the index files of the resulting segment.
    // If offset is in [minOffset, maxOffset + lastSegmentSize], truncate the containing segment and delete those to its right;
    // otherwise delete everything and create a new empty segment starting at offset.
    // Index file contents are NOT truncated; the caller must adjust them.
    // Only available in write mode.
    default CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        throw new UnsupportedOperationException();
    }
    default Map<String, AsyncFile> truncateSync(AsyncSegmentFile file, long offset) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Void> close(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default void closeSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    // Only available in write mode.
    default CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default void fsyncSync(AsyncSegmentFile file) {
        throw new UnsupportedOperationException();
    }
    default CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }
    default long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }
}
