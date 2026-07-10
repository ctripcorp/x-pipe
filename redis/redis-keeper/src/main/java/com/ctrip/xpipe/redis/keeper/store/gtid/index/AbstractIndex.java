package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.utils.DefaultControllableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractIndex {

    protected AtomicBoolean isClosed = new AtomicBoolean(false);

    public static final String BLOCK = "block_";
    public static final String INDEX = "index_";

    public static final String INDEX_V2 = "indexv2_";
    public static final String BLOCK_V2 = "blockv2_";

    private String fileName;
    private String baseDir;

    protected ControllableFile indexFile;

    public AbstractIndex(String baseDir, String fileName) {
        this.fileName = fileName;
        this.baseDir = baseDir;
    }

    public void initIndexFile() throws IOException {
        this.indexFile = new DefaultControllableFile(generateIndexName());
    }

    public abstract void init() throws IOException ;

    protected String getIndexPrefix(){
        return INDEX;
    }

    protected String getBlockPrefix(){
        return BLOCK;
    }

    String generateBlockName() {
        return Paths.get(baseDir ,getBlockPrefix() + fileName).toString();
    }

    String generateIndexName() {
        return Paths.get(baseDir ,getIndexPrefix() + fileName).toString();
    }

    String generateCmdFileName() {
        return Paths.get(baseDir ,fileName).toString();
    }

    protected File getFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    public static File findFloorIndexFileByOffset(String baseDir, long currentOffset) {
        return findFloorIndexFileByOffset(baseDir,currentOffset,INDEX);
    }

    public static File findFloorIndexFileByOffset(String baseDir, long currentOffset,String prefix) {
        File directory = new File(baseDir);
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("is not a directory");
        }
        File[] files = directory.listFiles((dir, name) -> name.matches(prefix+".*\\d+$"));
        if (files == null || files.length == 0) {
            return null;
        }

        File targetFile = Arrays.stream(files)
                .filter(file -> {
                    String fileName = file.getName();
                    long offset = extractOffset(fileName);
                    return offset < currentOffset || currentOffset < 0;
                })
                .max(Comparator.comparingLong(file -> {
                    String fileName = file.getName();
                    return Long.parseLong(fileName.substring(fileName.lastIndexOf('_') + 1));
                }))
                .orElse(null);

        return targetFile;
    }


    public static File findFirstIndexFileByOffset(String baseDir) {
        return findFirstIndexFileByOffset(baseDir,INDEX);
    }

    public static File findFirstIndexFileByOffset(String baseDir,String prefix) {
        File directory = new File(baseDir);
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("is not a directory");
        }
        File[] files = directory.listFiles((dir, name) -> name.matches(prefix+".*\\d+$"));
        if (files == null || files.length == 0) {
            return null;
        }

        File targetFile = Arrays.stream(files)
                .min(Comparator.comparingLong(file -> {
                    String fileName = file.getName();
                    return Long.parseLong(fileName.substring(fileName.lastIndexOf('_') + 1));
                }))
                .orElse(null);

        return targetFile;
    }


    public static long extractOffset(String fileName) {
        if(fileName.contains("_")) {
            return Long.parseLong(fileName.substring(fileName.lastIndexOf("_") + 1));
        } else {
            return  Long.parseLong(fileName);
        }
    }

    public boolean changeToPre() throws IOException {
        File pre = findFloorIndexFileByOffset(baseDir, extractOffset(fileName), getIndexPrefix());
        if(pre == null) {
            return false;
        }
        fileName = pre.getName().replace(getIndexPrefix(), "");
        closeIndexFile();
        indexFile = new DefaultControllableFile(pre);
        this.init();
        return true;
    }

        public File findNextFile() {
        File directory = new File(baseDir);
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("is not a directory");
        }
        File[] files = directory.listFiles((dir, name) -> name.matches(getIndexPrefix()+".*\\d+$"));
        if (files == null || files.length == 0) {
            return null;
        }

        long currentOffset = extractOffset(fileName);
        File nextFile = Arrays.stream(files)
                .filter(file -> {
                    String fileName = file.getName();
                    long offset = extractOffset(fileName);
                    return offset > currentOffset;
                })
                .min(Comparator.comparingLong(file -> {
                    String fileName = file.getName();
                    return Long.parseLong(fileName.substring(fileName.lastIndexOf('_') + 1));
                }))
                .orElse(null);

        return nextFile;
    }

    public boolean changeToNext() throws IOException {
        File nextFile = findNextFile();
        if (nextFile == null) {
            return false;
        }

        fileName = nextFile.getName().replace(getIndexPrefix(), "");
        closeIndexFile();
        indexFile = new DefaultControllableFile(nextFile);
        this.init();
        return true;
    }

    protected IndexEntry readPreIIndexEntry(IndexEntry currentEntry) throws IOException {
        long preIndex = currentEntry.getPosition() - getSegmentLength();
        if(preIndex <= 0) {
            return null;
        }
        this.indexFile.getFileChannel().position(preIndex);
        IndexEntry result = readIndexEntryFromFile(this.indexFile.getFileChannel());
        result.setPosition(preIndex);
        return result;
    }

    protected int getSegmentLength() {
        return IndexEntry.SEGMENT_LENGTH;
    }

    protected IndexEntry readIndexEntryFromFile(java.nio.channels.FileChannel channel) throws IOException {
        return IndexEntry.readFromFile(channel);
    }

    public void closeIndexFile() throws IOException {
        this.indexFile.close();
    }

    public String getFileName() {
        return fileName;
    }

    public long getStartOffset() {
        return extractOffset(fileName);
    }

    public String getBaseDir() {
        return baseDir;
    }
}
