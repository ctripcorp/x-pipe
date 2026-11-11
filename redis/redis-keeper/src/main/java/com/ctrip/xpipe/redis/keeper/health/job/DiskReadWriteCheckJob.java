package com.ctrip.xpipe.redis.keeper.health.job;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTelemetry;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTelemetryContext;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.File;
import java.nio.charset.StandardCharsets;

/** Write and read one probe file directly through the backing filesystem. */
public class DiskReadWriteCheckJob extends AbstractCommand<Boolean> {

    private static final String DISK_CHECK_DIR = "disk_check";

    private final AsyncFileSystem asyncFileSystem;
    private final String storePath;
    private final String hostname;
    private final IoTelemetry telemetry;

    public DiskReadWriteCheckJob(AsyncFileSystem asyncFileSystem, String storePath, String hostname) {
        this(asyncFileSystem, storePath, hostname, IoTelemetry.INSTANCE);
    }

    public DiskReadWriteCheckJob(AsyncFileSystem asyncFileSystem, String storePath,
            String hostname, IoTelemetry telemetry) {
        this.asyncFileSystem = asyncFileSystem;
        this.storePath = storePath;
        this.hostname = hostname;
        this.telemetry = telemetry == null ? IoTelemetry.INSTANCE : telemetry;
    }

    @Override
    protected void doExecute() {
        String dir = new File(storePath, DISK_CHECK_DIR).getPath();
        String path = new File(dir, hostname).getPath();
        byte[] data = String.valueOf(System.currentTimeMillis() / 1000)
                .getBytes(StandardCharsets.UTF_8);
        long startNanos = System.nanoTime();
        boolean success = false;
        AsyncFile writeHandle = null;
        AsyncFile readHandle = null;
        IoTelemetryContext.Scope probeScope = IoTelemetryContext.probe();
        try {
            AsyncFileSystemHelper.await(
                    () -> asyncFileSystem.mkdir(dir, true),
                    "disk check mkdir " + dir);

            writeHandle = AsyncFileSystemHelper.awaitOpen(
                    asyncFileSystem,
                    () -> asyncFileSystem.open(
                            path,
                            AbstractStorageFile.OpenMode.WRITE,
                            AbstractStorageFile.ReplaceMode.NORMAL,
                            true,
                            null,
                            AbstractStorageFile.CacheMode.NO_CACHE),
                    "disk check open write " + path);
            final AsyncFile write = writeHandle;
            AsyncFileSystemHelper.await(
                    () -> asyncFileSystem.truncate(write, 0),
                    "disk check truncate " + path);
            AsyncFileSystemHelper.writeAllBytes(
                    asyncFileSystem, write, data, "disk check write " + path);
            AsyncFileSystemHelper.await(
                    () -> asyncFileSystem.fsync(write),
                    "disk check fsync " + path);
            AsyncFileSystemHelper.closeHandle(
                    asyncFileSystem, write, null, "disk check close write " + path);
            writeHandle = null;

            readHandle = AsyncFileSystemHelper.awaitOpen(
                    asyncFileSystem,
                    () -> asyncFileSystem.open(
                            path,
                            AbstractStorageFile.OpenMode.READ,
                            AbstractStorageFile.ReplaceMode.NORMAL,
                            true,
                            null,
                            AbstractStorageFile.CacheMode.NO_CACHE),
                    "disk check open read " + path);
            final AsyncFile read = readHandle;
            long size = AsyncFileSystemHelper.await(
                    () -> asyncFileSystem.size(read),
                    "disk check size " + path);
            String result = AsyncFileSystemHelper.readAllUtf8(
                    asyncFileSystem, read, size, 0, "disk check read " + path);
            success = StringUtil.trimEquals(
                    new String(data, StandardCharsets.UTF_8), result);
            future().setSuccess(success);
        } catch (Throwable th) {
            getLogger().warn("[disk check read/write fail] {}", path, th);
            future().setSuccess(false);
        } finally {
            try {
                AsyncFileSystemHelper.closeHandle(
                        asyncFileSystem, writeHandle, null,
                        "disk check close write " + path);
                AsyncFileSystemHelper.closeReadHandle(
                        asyncFileSystem, readHandle, null,
                        "disk check close read " + path);
            } finally {
                telemetry.recordProbeRound(
                        path, System.nanoTime() - startNanos, success);
                probeScope.close();
            }
        }
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
