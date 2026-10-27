package com.ctrip.xpipe.redis.keeper.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ctrip.xpipe.redis.keeper.storage.FileFaultInjector.Category;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class DebugFileChannelTest {

    private static final String PATH = "/debug/file";

    private interface ChannelOp {
        void run(FileChannel channel) throws IOException;
    }

    // Every injected method with the category it is injected as.
    private static final Map<String, Category> INJECTED = new LinkedHashMap<>();
    private static final Map<String, ChannelOp> OPS = new LinkedHashMap<>();

    static {
        injected("read", Category.READ, ch -> ch.read(ByteBuffer.allocate(1)));
        injected("read[]", Category.READ, ch -> ch.read(new ByteBuffer[]{ByteBuffer.allocate(1)}, 0, 1));
        injected("pread", Category.READ, ch -> ch.read(ByteBuffer.allocate(1), 0));
        injected("transferTo", Category.READ, ch -> ch.transferTo(0, 1, Channels.newChannel(new ByteArrayOutputStream())));
        injected("write", Category.WRITE, ch -> ch.write(ByteBuffer.allocate(1)));
        injected("write[]", Category.WRITE, ch -> ch.write(new ByteBuffer[]{ByteBuffer.allocate(1)}, 0, 1));
        injected("pwrite", Category.WRITE, ch -> ch.write(ByteBuffer.allocate(1), 0));
        injected("transferFrom", Category.WRITE,
                ch -> ch.transferFrom(Channels.newChannel(new ByteArrayInputStream(new byte[1])), 0, 1));
        injected("truncate", Category.WRITE, ch -> ch.truncate(0));
        injected("force", Category.FORCE, ch -> ch.force(true));
    }

    private static void injected(String name, Category category, ChannelOp op) {
        INJECTED.put(name, category);
        OPS.put(name, op);
    }

    private MutableDebugFileChannelConfig config;
    private FileFaultInjector injector;
    private Path tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("debug-channel-test-");
        config = new MutableDebugFileChannelConfig();
        injector = new FileFaultInjector(config);
    }

    @After
    public void tearDown() {
        injector.close();
        deleteRecursively(tempDir.toFile());
    }

    @Test
    public void testPassThroughWithoutInjection() throws Exception {
        Path file = tempDir.resolve("data");
        try (FileChannel ch = new DebugFileChannelFactory(injector).open(file,
                EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
            assertTrue(ch instanceof DebugFileChannel);
            assertEquals(2, ch.write(ByteBuffer.wrap(new byte[]{1, 2})));
            assertEquals(2, ch.write(new ByteBuffer[]{ByteBuffer.wrap(new byte[]{3}), ByteBuffer.wrap(new byte[]{4})}));
            assertEquals(1, ch.write(ByteBuffer.wrap(new byte[]{9}), 4));
            assertEquals(1, ch.transferFrom(Channels.newChannel(new ByteArrayInputStream(new byte[]{5})), 5, 1));
            assertEquals(6, ch.size());
            assertSame(ch, ch.position(0));
            assertEquals(0, ch.position());

            ByteBuffer first = ByteBuffer.allocate(2);
            assertEquals(2, ch.read(first));
            ByteBuffer[] scatter = {ByteBuffer.allocate(1), ByteBuffer.allocate(1)};
            assertEquals(2, ch.read(scatter, 0, 2));
            ByteBuffer at = ByteBuffer.allocate(2);
            assertEquals(2, ch.read(at, 4));
            assertArrayEquals(new byte[]{1, 2}, first.array());
            assertArrayEquals(new byte[]{3, 4}, new byte[]{scatter[0].get(0), scatter[1].get(0)});
            assertArrayEquals(new byte[]{9, 5}, at.array());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            WritableByteChannel target = Channels.newChannel(out);
            assertEquals(6, ch.transferTo(0, 6, target));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 9, 5}, out.toByteArray());

            MappedByteBuffer mapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, 6);
            assertEquals(9, mapped.get(4));
            try (FileLock lock = ch.tryLock(0, 6, false)) {
                assertNotNull(lock);
            }
            ch.force(true);
            assertSame(ch, ch.truncate(3));
            assertEquals(3, ch.size());
        }
    }

    @Test
    public void testInjectedMethodsFailBeforeReachingDelegate() throws Exception {
        config.eio = true;
        injector.refresh();
        for (Map.Entry<String, ChannelOp> op : OPS.entrySet()) {
            FileChannel delegate = mock(FileChannel.class);
            DebugFileChannel ch = new DebugFileChannel(delegate, PATH, injector);
            try {
                op.getValue().run(ch);
                fail(op.getKey() + " should be injected");
            } catch (IOException e) {
                assertEquals(op.getKey(), FileFaultInjector.EIO_MESSAGE, e.getMessage());
            }
            verifyNoInteractions(delegate);
        }
    }

    @Test
    public void testEachMethodInjectedOnlyByItsCategory() throws Exception {
        config.eio = true;
        for (Category category : Category.values()) {
            config.ops = category.name();
            injector.refresh();
            for (Map.Entry<String, ChannelOp> op : OPS.entrySet()) {
                DebugFileChannel ch = new DebugFileChannel(mock(FileChannel.class), PATH, injector);
                boolean injected = INJECTED.get(op.getKey()) == category;
                try {
                    op.getValue().run(ch);
                    assertFalse(op.getKey() + " under " + category, injected);
                } catch (IOException e) {
                    assertTrue(op.getKey() + " under " + category, injected);
                }
            }
        }
    }

    @Test
    public void testNotInjectedMethodsForward() throws Exception {
        config.eio = true;
        injector.refresh();
        FileChannel delegate = mock(FileChannel.class);
        Mockito.when(delegate.size()).thenReturn(7L);
        Mockito.when(delegate.position()).thenReturn(3L);
        DebugFileChannel ch = new DebugFileChannel(delegate, PATH, injector);

        assertEquals(7L, ch.size());
        assertEquals(3L, ch.position());
        assertSame(ch, ch.position(5));
        ch.map(FileChannel.MapMode.READ_ONLY, 0, 1);
        ch.lock(0, 1, true);
        ch.tryLock(0, 1, false);

        verify(delegate).position(5);
        verify(delegate).map(FileChannel.MapMode.READ_ONLY, 0, 1);
        verify(delegate).lock(0, 1, true);
        verify(delegate).tryLock(0, 1, false);
    }

    @Test
    public void testForwardedArgumentsWhenNotInjected() throws Exception {
        FileChannel delegate = mock(FileChannel.class);
        DebugFileChannel ch = new DebugFileChannel(delegate, PATH, injector);
        for (ChannelOp op : OPS.values()) {
            op.run(ch);
        }
        verify(delegate).read(any(ByteBuffer.class));
        verify(delegate).read(any(ByteBuffer[].class), anyInt(), anyInt());
        verify(delegate).read(any(ByteBuffer.class), anyLong());
        verify(delegate).transferTo(anyLong(), anyLong(), any(WritableByteChannel.class));
        verify(delegate).write(any(ByteBuffer.class));
        verify(delegate).write(any(ByteBuffer[].class), anyInt(), anyInt());
        verify(delegate).write(any(ByteBuffer.class), anyLong());
        verify(delegate).transferFrom(any(ReadableByteChannel.class), anyLong(), anyLong());
        verify(delegate).truncate(0);
        verify(delegate).force(anyBoolean());
    }

    @Test
    public void testCloseUnderEioClosesRealChannel() throws Exception {
        FileChannel real = FileChannel.open(tempDir.resolve("close"),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        DebugFileChannel ch = new DebugFileChannel(real, PATH, injector);
        config.eio = true;
        injector.refresh();

        ch.close();
        assertFalse(real.isOpen());
        assertFalse(ch.isOpen());
        ch.close();
    }

    @Test
    public void testFactoryOpenInjection() throws Exception {
        DebugFileChannelFactory factory = new DebugFileChannelFactory(injector);
        assertSame(injector, factory.getInjector());
        Path file = tempDir.resolve("opened");
        config.eio = true;
        config.ops = "OPEN";
        injector.refresh();
        try {
            factory.open(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
            fail("open should be injected");
        } catch (IOException e) {
            assertTrue(StorageUtil.wrapIOException(e) instanceof EIOException);
        }
        assertFalse("file not created when open is injected", Files.exists(file));

        config.ops = "WRITE";
        injector.refresh();
        try (FileChannel ch = factory.open(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
            assertTrue(Files.exists(file));
            try {
                ch.write(ByteBuffer.wrap(new byte[]{1}));
                fail("write should be injected");
            } catch (IOException expected) {
                assertEquals(0, ch.size());
            }
        }
    }

    @Test
    public void testFactoryAppliesPathFilter() throws Exception {
        DebugFileChannelFactory factory = new DebugFileChannelFactory(injector);
        config.eio = true;
        config.ops = "OPEN";
        config.pathContains = "replA";
        injector.refresh();
        try (FileChannel ch = factory.open(tempDir.resolve("replB"),
                EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
            assertNotNull(ch);
        }
        try {
            factory.open(tempDir.resolve("replA"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
            fail("replA open should be injected");
        } catch (IOException expected) {
            assertTrue(Arrays.asList(tempDir.toFile().list()).contains("replB"));
        }
    }

    private static void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursively(c);
            }
        }
        f.delete();
    }
}
