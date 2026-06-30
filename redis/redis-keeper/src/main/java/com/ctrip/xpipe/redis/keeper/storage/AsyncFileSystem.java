package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

// currently only support append only mode and atomic replace mode.
// All read/write operations attempt to read/write as much as possible until EOF, I/O error, or completion.
// TODO to prevent unnessery memory copy, we should reconsider the interface design.
public interface AsyncFileSystem {
    void shutdown();

    // ---- AsyncFile ----
    // lenient: if true and path is not a regular file, I/O operations will throw NPE
    CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace, boolean lenient);
    CompletableFuture<Boolean> isFile(AsyncFile file);
    CompletableFuture<Boolean> isDirectory(String path);
    CompletableFuture<Long> lastModified(AsyncFile file);
    // Only available in read mode.
    CompletableFuture<Void> position(AsyncFile file, long position);
    CompletableFuture<Long> read(AsyncFile file, long length, long offset, byte[] buffer);
    CompletableFuture<Long> read(AsyncFile file, long length, byte[] buffer);
    CompletableFuture<Long> write(AsyncFile file, byte[] data, long length);
    CompletableFuture<Void> delete(String path);
    CompletableFuture<Boolean> exists(String path);
    CompletableFuture<Long> size(AsyncFile file);
    CompletableFuture<Boolean> mkdir(String path, boolean recursive);
    CompletableFuture<Boolean> rmdir(String path, boolean recursive);
    // Only available in write mode.
    CompletableFuture<Boolean> truncate(AsyncFile file, long size);
    CompletableFuture<Void> close(AsyncFile file);
    // Only available in write mode.
    CompletableFuture<Void> fsync(AsyncFile file);
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
    // For a newly created (empty) segment file in write mode, truncate(offset) must be called once to specify the initial offset before writing.
    CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<IndexFileMapping> indexMappings, boolean write);
    // Only available in read mode.
    CompletableFuture<Void> position(AsyncSegmentFile file, long offset);
    CompletableFuture<Long> read(AsyncSegmentFile file, long length, byte[] buffer);
    CompletableFuture<Long> write(AsyncSegmentFile file, byte[] data, long length);
    // Only available in write mode.
    CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file);
    List<Long> list(AsyncSegmentFile file);
    long getCurrentSegmentStartOffset(AsyncSegmentFile file);
    // position to one segment and then call this method to get the current index files.
    CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes);
    CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file);
    // open index files by startOffset. no need to position to the startOffset. should close index files when done.
    // Only available in read mode.
    CompletableFuture<Map<String, AsyncFile>> openIndexFiles(AsyncSegmentFile file, long startOffset);
    CompletableFuture<Long> size(AsyncSegmentFile file);
    CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset);
    CompletableFuture<Long> lastModified(AsyncSegmentFile file);
    CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset);
    // startOffsets must be ordered and contiguous from the first offset, will delete segments accordingly.
    // Cannot delete the last segment.
    // Only available in write mode.
    CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets);
    // Delete all known segment and index files.
    // Only available in write mode.
    CompletableFuture<Void> delete(AsyncSegmentFile file);
    // Truncate at logical offset, returning the index files of the resulting segment.
    // If offset is in [minOffset, maxOffset + lastSegmentSize], truncate the containing segment and delete those to its right;
    // otherwise delete everything and create a new empty segment starting at offset.
    // Index file contents are NOT truncated; the caller must adjust them.
    // Only available in write mode.
    CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset);
    CompletableFuture<Void> close(AsyncSegmentFile file);
    // Only available in write mode.
    CompletableFuture<Void> fsync(AsyncSegmentFile file);
    // Mixing transferTo with read()/position() on the same AsyncSegmentFile is NOT supported
    // as transferTo also requires a position to be set.
    CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target);
}
