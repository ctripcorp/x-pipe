package com.ctrip.xpipe;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.DefaultExceptionHandler;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.testutils.ByteBufReleaseWrapper;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkTestServer;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *         <p>
 *         2016年3月28日 下午5:44:47
 */
public class AbstractTest {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    public static String KEY_INCRMENTAL_ZK_PORT = "INCRMENTAL_ZK_PORT";

    protected ExecutorService executors;

    protected ScheduledExecutorService scheduled;

    private ComponentRegistry componentRegistry;

    public static final String LOCAL_HOST = "127.0.0.1";

    protected LoopResources loopResources;

    protected ConnectionProvider connectionProvider;

    @Rule
    public TestName name = new TestName();

    private static Properties properties = new Properties();

    private Properties orginProperties;

    private ComponentRegistry startedComponentRegistry;

    protected void doBeforeAbstractTest() throws Exception {
    }

    @BeforeClass
    public static void beforeAbstractTestClass() {
        if (System.getProperty(CatConfig.CAT_ENABLED_KEY) == null) {
            System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
        }
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
    }

    @Before
    public void beforeAbstractTest() throws Exception {

        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        Thread.interrupted();//clear interrupt

        executors = Executors.newCachedThreadPool(XpipeThreadFactory.create(getTestName()));
        scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create(getTestName()));

        loopResources = LoopResources.create("TestHttpLoop", LoopResources.DEFAULT_IO_WORKER_COUNT, true);
        connectionProvider = ConnectionProvider.builder("TestConnProvider").maxConnections(100)
                .pendingAcquireTimeout(Duration.ofMillis(1000)).maxIdleTime(Duration.ofMillis(10000)).build();

        orginProperties = (Properties) System.getProperties().clone();

        doBeforeAbstractTest();


        logger.info(remarkableMessage("[begin test][{}]"), name.getMethodName());

        componentRegistry = new DefaultRegistry(new CreatedComponentRedistry(), getSpringRegistry());

        startedComponentRegistry = new CreatedComponentRedistry();
        startedComponentRegistry.initialize();
        startedComponentRegistry.start();

        Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());
        InputStream fins = getClass().getClassLoader().getResourceAsStream("xpipe-test.properties");
        try {
            properties.load(fins);
        } finally {
            if (fins != null) {
                fins.close();
            }
        }

        File file = new File(getTestFileDir());
        if (file.exists() && deleteTestDirBeforeTest()) {
            deleteTestDir();
        }

        if (!file.exists()) {
            boolean testSucceed = file.mkdirs();
            if (!testSucceed) {
                throw new IllegalStateException("test dir make failed!" + file);
            }
        }
    }

    protected ByteBuf directByteBuf() {

        return directByteBuf(1 << 10);
    }

    protected ByteBuf directByteBuf(int size) {

        ByteBuf byteBuf = allocator.directBuffer(size);
        addReleasable(byteBuf);

        return byteBuf;
    }

    protected void addReleasable(Object object) {

        if (object instanceof ByteBuf) {
            try {
                add(new ByteBufReleaseWrapper((ByteBuf) object));
            } catch (Exception e) {
                throw new IllegalStateException("add " + object, e);
            }
            return;
        }

        throw new IllegalArgumentException("unknown:" + object);
    }

    protected String getTestName() {
        return name.getMethodName();
    }

    protected void shouldThrowException(Runnable runnable) {
        try{
            runnable.run();
            Assert.fail();
        }catch (Exception e){
            logger.info("shouldThrowException: {}", e.getMessage());
        }
    }



    protected void waitConditionUntilTimeOut(BooleanSupplier booleanSupplier) throws TimeoutException {

        waitConditionUntilTimeOut(booleanSupplier, 5000, 2);
    }

    protected void waitConditionUntilTimeOut(BooleanSupplier booleanSupplier, int waitTimeMilli) throws TimeoutException {

        waitConditionUntilTimeOut(booleanSupplier, waitTimeMilli, 2);
    }

    protected void waitConditionUntilTimeOut(BooleanSupplier booleanSupplier, int waitTimeMilli, int intervalMilli) throws TimeoutException {

        long maxTime = System.currentTimeMillis() + waitTimeMilli;


        while (true) {
            boolean result = booleanSupplier.getAsBoolean();
            if (result) {
                return;
            }
            if (System.currentTimeMillis() >= maxTime) {
                throw new TimeoutException("timeout still false:" + waitTimeMilli);
            }
            sleep(intervalMilli);
        }
    }

    protected boolean deleteTestDirBeforeTest() {
        return true;
    }

    private ComponentRegistry getSpringRegistry() {

        ConfigurableApplicationContext applicationContext = createSpringContext();
        if (applicationContext != null) {
            return new SpringComponentRegistry(applicationContext);
        }
        return null;
    }

    protected XpipeNettyClientKeyedObjectPool getXpipeNettyClientKeyedObjectPool() throws Exception {

        XpipeNettyClientKeyedObjectPool result;

        try {
            result = getBean(XpipeNettyClientKeyedObjectPool.class);
        } catch (Exception e) {
            result = new XpipeNettyClientKeyedObjectPool();
            add(result);
        }

        LifecycleHelper.initializeIfPossible(result);
        LifecycleHelper.startIfPossible(result);
        return result;
    }


    /**
     * to be overriden by subclasses
     *
     * @return
     */
    protected ConfigurableApplicationContext createSpringContext() {
        return null;
    }

    protected <T> T getBean(Class<T> clazz) {

        return getRegistry().getComponent(clazz);
    }

    protected void initRegistry() throws Exception {

        componentRegistry.initialize();
    }

    protected void startRegistry() throws Exception {

        componentRegistry.start();
    }

    public static String randomString() {

        return randomString(1 << 10);
    }

    public static String randomString(int length) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + (int) (26 * Math.random())));
        }

        return sb.toString();
    }

    protected String getRealDir(final String dir) {

        String userHome = getUserHome();

        String newDir = dir;
        if (dir != null) {
            newDir = dir.replace("~", userHome);
        }

        return newDir;
    }

    protected String getTestFileDir() {

        String result = getUserHome() + "/test";
        String testDir = properties.getProperty("test.file.dir");
        if (testDir != null) {
            result = getRealDir(testDir);
        }

        return result + "/" + getClass().getSimpleName() + "-" + currentTestName();
    }

    public static String getUserHome() {

        return System.getProperty("user.home");
    }

    protected void sleepSeconds(int seconds) {
        sleep(seconds * 1000);
    }

    protected void sleepIgnoreInterrupt(int time) {
        long future = System.currentTimeMillis() + time;

        while (true) {
            long left = future - System.currentTimeMillis();
            if (left <= 0) {
                break;
            }
            if (left > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(left);
                } catch (InterruptedException e) {
                }
            }
        }

    }

    protected void sleep(int miliSeconds) {

        try {
            TimeUnit.MILLISECONDS.sleep(miliSeconds);
        } catch (InterruptedException e) {
        }
    }

    protected String readFileAsString(String fileName) {

        return readFileAsString(fileName, Codec.defaultCharset);
    }

    protected String readFileAsString(String fileName, Charset charset) {

        FileInputStream fins = null;
        try {
            byte[] data = new byte[2048];
            ByteArrayOutputStream baous = new ByteArrayOutputStream();
            fins = new FileInputStream(new File(fileName));

            while (true) {
                int size = fins.read(data);
                if (size > 0) {
                    baous.write(data, 0, size);
                }
                if (size == -1) {
                    break;
                }
            }
            return new String(baous.toByteArray(), charset);
        } catch (FileNotFoundException e) {
            logger.error("[readFileAsString]" + fileName, e);
        } catch (IOException e) {
            logger.error("[readFileAsString]" + fileName, e);
        } finally {
            if (fins != null) {
                try {
                    fins.close();
                } catch (IOException e) {
                    logger.error("[readFileAsString]", e);
                }
            }
        }
        return null;
    }

    protected void add(Object lifecycle) throws Exception {
        this.componentRegistry.add(lifecycle);
    }

    protected void remove(Object lifecycle) throws Exception {
        this.componentRegistry.remove(lifecycle);
    }

    public ComponentRegistry getRegistry() {
        return componentRegistry;
    }

    protected String currentTestName() {
        return name.getMethodName();
    }

    public static int portUsable(int fromPort) {

        for (int i = fromPort; i < fromPort + 100; i++) {
            if (isUsable(i)) {
                return i;
            }
        }
        throw new IllegalStateException("unfonud usable port from %d" + fromPort);
    }

    public static int randomPort() {
        return randomPort(10000, 30000, null);
    }

    public static Set<Integer> usedPorts = new HashSet<>();

    public static Set<Integer> randomPorts(int count) {

        Set<Integer> result = new HashSet<>();
        for (int i = 0; i < count; i++) {
            result.add(randomPort(result));
        }
        return result;
    }


    public static int randomPort(Set<Integer> different) {
        return randomPort(10000, 30000, different);
    }

    /**
     * find an available port from min to max
     *
     * @param min
     * @param max
     * @return
     */
    public static int randomPort(int min, int max) {

        return randomPort(min, max, null);
    }

    public static synchronized int randomPort(int min, int max, Set<Integer> different) {

        Random random = new Random();

        for (int i = min; i <= max; i++) {
            int port = min + random.nextInt(max - min + 1);
            if ((different == null || !different.contains(new Integer(port))) && isUsable(port) && !usedPorts.contains(port) ) {
                usedPorts.add(port);
                return port;
            }
        }

        throw new IllegalStateException(String.format("random port not found:(%d, %d)", min, max));
    }

    protected static boolean isUsable(int port) {

        try (ServerSocket s = new ServerSocket()) {
            s.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException e) {
        }
        return false;
    }

    protected static int incrementalPort(int begin) {
        return incrementalPort(begin, Integer.MAX_VALUE);
    }

    protected static int incrementalPort(int begin, int end) {

        for (int i = begin; i <= end; i++) {
            if (isUsable(i)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("can not find usable port [%d, %d]", begin, end));
    }

    protected int randomInt() {

        Random random = new Random();
        return random.nextInt();
    }

    protected int randomInt(int start, int end) {

        Random random = new Random();
        return start + random.nextInt(end - start + 1);
    }

    protected String remarkableMessage(String msg) {
        return String.format("--------------------------%s--------------------------\r\n", msg);
    }

    protected void waitForAnyKeyToExit() throws IOException {
        System.out.println("type any key to exit..................");
        waitForAnyKey();
    }

    protected void waitForAnyKey() throws IOException {
        System.in.read();
    }

    protected ZkTestServer startRandomZk() {

        int zkPort = incrementalPort(2181, 2281);
        return startZk(zkPort);
    }

    protected ZkTestServer startZk(int zkPort) {
        try {
            logger.info(remarkableMessage("[startZK]{}"), zkPort);
            ZkTestServer zkTestServer = new ZkTestServer(zkPort);
            zkTestServer.initialize();
            zkTestServer.start();
            add(zkTestServer);
            return zkTestServer;
        } catch (Exception e) {
            logger.error("[startZk]", e);
            throw new IllegalStateException("[startZk]" + zkPort, e);
        }
    }

    protected int getTestZkPort() {

        boolean incremental = Boolean.parseBoolean(System.getProperty(KEY_INCRMENTAL_ZK_PORT, "false"));
        if (incremental) {
            return incrementalPort(defaultZkPort());
        }
        return randomPort();

    }

    public static int defaultZkPort() {
        return 2181;
    }

    public static int defaultMetaServerPort() {
        return 9747;
    }

    protected DefaultEndPoint localhostEndpoint(int port) {
        return new DefaultEndPoint("localhost", port);
    }

    protected InetSocketAddress localhostInetAdress(int port) {
        return new InetSocketAddress("localhost", port);
    }

    protected DefaultEndPoint localHostEndpoint(int port) {
        return new DefaultEndPoint("localhost", port);
    }

    protected HostPort localHostport(int port) {
        return new HostPort("localhost", port);
    }

    protected Server startEmptyServer() throws Exception {
        return startServer(new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {

                return new AbstractIoAction(socket) {

                    @Override
                    protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                    }

                    @Override
                    protected Object doRead(InputStream ins) throws IOException {

                        while (true) {

                            int data = ins.read();
                            if (data == -1) {
                                break;
                            }
                            System.out.print((char) data);
                        }
                        return null;
                    }
                };
            }
        });
    }


    protected Server startEchoServer() throws Exception {
        return startEchoServer(randomPort(), null);
    }

    protected Server startEchoPrefixServer(String prefix) throws Exception {
        return startEchoServer(randomPort(), prefix);
    }

    protected Server startEchoServer(int port) throws Exception {
        return startEchoServer(port, null);
    }

    protected Server startEchoServer(int port, String prefix) throws Exception {
        return startServer(port, new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {
                return new AbstractIoAction(socket) {

                    private String line;

                    @Override
                    protected Object doRead(InputStream ins) throws IOException {
                        line = readLine(ins);
                        logger.debug("[doRead]{}", line);
                        logger.info("[doRead]{}", line == null ? null : line.length());
                        return line;
                    }

                    @Override
                    protected void doWrite(OutputStream ous, Object readResult) throws IOException {

                        String[] sp = line.split("\\s+");
                        if (sp.length >= 1) {
                            if (sp[0].equalsIgnoreCase("sleep")) {
                                int sleep = Integer.parseInt(sp[1]);
                                logger.info("[sleep]{}", sleep);
                                sleepIgnoreInterrupt(sleep);
                            }
                        }
                        logger.debug("[doWrite]{}", line.length());
                        logger.debug("[doWrite]{}", line);
                        if (prefix != null) {
                            ous.write(prefix.getBytes());
                        }
                        ous.write(line.getBytes());
                        ous.flush();
                    }
                };
            }
        });
    }

    protected Server startServer(int serverPort, IoActionFactory ioActionFactory) throws Exception {

        Server server = new Server(serverPort, ioActionFactory);
        server.initialize();
        server.start();

        add(server);
        return server;

    }

    protected Server startServer(IoActionFactory ioActionFactory) throws Exception {
        return startServer(randomPort(), ioActionFactory);
    }

    protected Server startServer(int serverPort, final String expected) throws Exception {
        return startServer(serverPort, new Callable<String>() {

            @Override
            public String call() throws Exception {
                return expected;
            }
        });
    }

    protected Server startServer(int serverPort, final Callable<String> function) throws Exception {

        return startServer(serverPort, new Function<String, String>() {
            @Override
            public String apply(String s) {
                try {
                    return function.call();
                } catch (Exception e) {
                    logger.error("[startServer]" + function, e);
                    return e.getMessage();
                }
            }
        });
    }

    protected Server startServer(int serverPort, final Function<String, String> function) throws Exception {
        IoActionFactory ioActionFactory = new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {
                return new AbstractIoAction(socket) {

                    private String readLine = null;

                    @Override
                    protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                        try {
                            String call = function.apply(readLine == null? null : readLine.trim());
                            if (call != null) {
                                ous.write(call.getBytes());
                            }
                        } catch (Exception e) {
                            throw new IllegalStateException("[doWrite]", e);
                        }
                    }

                    @Override
                    protected Object doRead(InputStream ins) throws IOException {
                        readLine = readLine(ins);
                        logger.info("[doRead]{}", readLine == null ? null : readLine.trim());
                        return readLine;
                    }
                };
            }
        };
        return startServer(serverPort, ioActionFactory);
    }

    protected Server startServer(final String result) throws Exception {
        return startServer(randomPort(), result);
    }

    protected Server startServerWithFlexibleResult(Callable<String> result) throws Exception {
        return startServer(randomPort(), result);
    }

    @After
    public void afterAbstractTest() throws Exception {

        try {
            logger.info(remarkableMessage("[end   test][{}]"), name.getMethodName());

            LifecycleHelper.stopIfPossible(componentRegistry);
            LifecycleHelper.disposeIfPossible(componentRegistry);
            componentRegistry.destroy();
        } catch (Exception e) {
            logger.error("[afterAbstractTest]", e);
        }


        try {
            if (deleteTestDirAfterTest()) {
                deleteTestDir();
            }
        } catch (Exception e) {
            logger.error("[afterAbstractTest][clean test dir]", e);
        }

        try {
            doAfterAbstractTest();
        } catch (Exception e) {
            logger.error("[afterAbstractTest]", e);
        }

        System.setProperties(orginProperties);

        executors.shutdownNow();
        scheduled.shutdownNow();
    }

    private void deleteTestDir() {
        File file = new File(getTestFileDir());
        FileUtils.deleteQuietly(file);
    }

    protected boolean deleteTestDirAfterTest() {

        return true;
    }

    protected void doAfterAbstractTest() throws Exception {
    }

    public static class BlockingCommand extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

        private int sleepTime;

        private int timeout;

        private volatile boolean processing = false;

        public BlockingCommand(int sleepTime) {
            this.sleepTime = sleepTime;
        }

        public boolean isProcessing() {
            return processing;
        }

        @Override
        protected void doExecute() throws Exception {
            processing = true;
            Thread.sleep(sleepTime);
            future().setSuccess();
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        @Override
        public int getCommandTimeoutMilli() {
            return timeout;
        }

        public BlockingCommand setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    public static class CountingCommand extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

        private AtomicInteger counter;

        private int sleepTime;

        private int timeout;

        public CountingCommand(AtomicInteger counter, int sleepTime) {
            this.counter = counter;
            this.sleepTime = sleepTime;
        }

        @Override
        protected void doExecute() throws Exception {
            Thread.sleep(sleepTime);
            counter.incrementAndGet();
            future().setSuccess();
        }

        @Override
        protected void doReset() {
            counter.decrementAndGet();
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        @Override
        public int getCommandTimeoutMilli() {
            return timeout;
        }

        public CountingCommand setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    protected String getTimeoutIp() {
        List<String> ipam = Lists.newArrayList("192.0.0.0", "192.0.0.1", "127.0.0.0", "10.0.0.1", "10.0.0.0");
        for(String ip : ipam) {
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(ip, 6379), 100);
            } catch (IOException e) {
                if(e instanceof SocketTimeoutException) {
                    return ip;
                }
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {

                }
            }

        }
        return "127.0.0.2";
    }

}
