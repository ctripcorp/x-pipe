package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

// currently only support append only mode and atomic replace mode.
// TODO to prevent unnessery memory copy, we should reconsider the interface design.
public interface AsyncFileSystem {
    CompletableFuture<AsyncFile> open(String path, boolean write, boolean atomicReplace);
    CompletableFuture<Boolean> isFile(AsyncFile file);
    CompletableFuture<Long> lastModified(AsyncFile file);
    CompletableFuture<Void> position(AsyncFile file, long position);
    CompletableFuture<Integer> read(AsyncFile file, long length, long offset, byte[] buffer);
    CompletableFuture<Integer> read(AsyncFile file, long length, byte[] buffer);
    CompletableFuture<Integer> write(AsyncFile file, byte[] data, long length);
    CompletableFuture<Void> delete(String path);
    CompletableFuture<Boolean> exists(String path);
    CompletableFuture<Long> size(AsyncFile file);
    CompletableFuture<Boolean> mkdir(String path, boolean recursive);
    CompletableFuture<Boolean> rmdir(String path, boolean recursive);
    CompletableFuture<Boolean> truncate(AsyncFile file, long size);
    CompletableFuture<Void> close(AsyncFile file);
    CompletableFuture<Void> fsync(AsyncFile file);
    // segment file return as raw file.
    CompletableFuture<List<String>> list(String path);
    CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target);

    // A segment file represents a list of files with the same prefix and monotonically increasing offsets.
    // Each segment file has 0 to n index files which share the same lifecycle.
    // Segment files operate using logical offsets instead of physical offsets.
    // It automatically deletes preceding non-contiguous segment files.
    // Write mode always opens the latest segment file and index files, and auto-closes them upon rollover.
    // Read mode opens the segment file after the first read, and opens index files when getCurrentIndexFiles() is called. They auto-close when reading the next segment.
    CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write);
    CompletableFuture<Void> position(AsyncSegmentFile file, long offset);
    CompletableFuture<Integer> read(AsyncSegmentFile file, long length, long offset, byte[] buffer);
    CompletableFuture<Integer> read(AsyncSegmentFile file, long length, byte[] buffer);
    CompletableFuture<Integer> write(AsyncSegmentFile file, byte[] data, long length);
    void roll(AsyncSegmentFile file);
    List<Long> list(AsyncSegmentFile file);
    // position to one segment and then call this method to get the current index files.
    CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes);
    CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file);
    // open index files by startOffset. no need to position to the startOffset. should close index files when done.
    Map<String, AsyncFile> openIndexFiles(AsyncSegmentFile file, long startOffset);
    CompletableFuture<Long> size(AsyncSegmentFile file);
    CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset);
    CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets);
    CompletableFuture<Void> truncate(AsyncSegmentFile file, long offset);
    CompletableFuture<Void> close(AsyncSegmentFile file);
    CompletableFuture<Void> fsync(AsyncSegmentFile file);
    CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target);
}
