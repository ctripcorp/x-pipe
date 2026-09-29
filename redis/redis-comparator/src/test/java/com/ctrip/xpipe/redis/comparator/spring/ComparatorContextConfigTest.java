package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.comparator.balance.CompareTaskAssigner;
import com.ctrip.xpipe.redis.comparator.balance.ServerGroupProvider;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.ComparatorMetaService;
import com.ctrip.xpipe.redis.comparator.meta.KeeperStreamFactory;
import com.ctrip.xpipe.redis.comparator.meta.PrepareWatchCache;
import com.ctrip.xpipe.redis.comparator.controller.ComparatorStatusController;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager;
import com.ctrip.xpipe.redis.comparator.report.CompareMetricsCollector;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

/**
 * T-MB.4 ②③：测试 profile 下上下文可启动（Production / CMS 不加载，不访问网络）；
 * CMS / ACK 挂 {@code SCHEDULED_EXECUTOR}；分片任务 refresh 挂专用 scheduled。
 * 不另起比对线程池。
 */
public class ComparatorContextConfigTest extends AbstractTest {

    @Override
    protected ConfigurableApplicationContext createSpringContext() {
        return new AnnotationConfigApplicationContext(ComparatorContextConfig.class);
    }

    @Test
    public void testContextStartsUnderTestProfileWithoutNetwork() {
        ComparatorConfig config = getBean(ComparatorConfig.class);
        Assert.assertNotNull(config);
        Assert.assertEquals(30000, config.getMetaRefreshIntervalMilli());

        Assert.assertNotNull(AbstractSpringConfigContext.getApplicationContext()
                .getBean(AbstractSpringConfigContext.SCHEDULED_EXECUTOR, ScheduledExecutorService.class));
        Assert.assertNotNull(AbstractSpringConfigContext.getApplicationContext()
                .getBean(AbstractSpringConfigContext.GLOBAL_EXECUTOR, ExecutorService.class));

        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(Production.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(org.springframework.web.client.RestTemplate.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(ServerGroupProvider.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(CompareTaskAssigner.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(ComparatorMetaService.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(ShardCompareTaskManager.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(PrepareWatchCache.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(KeeperStreamFactory.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(CompareMetricsCollector.class).size());
        Assert.assertEquals(1, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(ComparatorStatusController.class).size());
    }

    @Test
    public void testNoCompareThreadPoolBean() {
        String[] names = AbstractSpringConfigContext.getApplicationContext().getBeanDefinitionNames();
        for (String name : names) {
            Assert.assertFalse("must not register compare thread pool: " + name,
                    name.toLowerCase().contains("compareexecutor")
                            || name.toLowerCase().contains("comparethread"));
        }
    }

    @Test
    public void testListeningPortBoundToServerPort() throws IOException {
        String text = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/spring/Production.java")),
                StandardCharsets.UTF_8);
        Assert.assertTrue("REPLCONF listening-port must follow HTTP server.port (D35 ④)",
                text.contains("${server.port:8080}"));
        Assert.assertFalse("must not hardcode DEFAULT_LISTENING_PORT in Production",
                text.contains("DEFAULT_LISTENING_PORT"));
    }

    @Test
    public void testPackageStartupScriptContract() throws IOException {
        Path script = packageRoot().resolve("src/main/scripts/startup.sh");
        Assert.assertTrue(script.toString(), Files.isRegularFile(script));
        String text = new String(Files.readAllBytes(script), StandardCharsets.UTF_8);
        Assert.assertTrue(text.contains("SAFE_PERCENT=50"));
        Assert.assertTrue(text.contains("MAX_MEM=8"));
        Assert.assertTrue(text.contains("MAX_DIRECT_MB=512"));
        Assert.assertTrue(text.contains("free -g"));
        Assert.assertTrue(text.contains("/health"));
        Assert.assertTrue(text.contains("getPortFromPathOrDefault $FULL_DIR 8080"));
        Assert.assertTrue("must test the jar path variable, not the literal PATH_TO_JAR",
                text.contains("! -f \"$PATH_TO_JAR\""));
        Assert.assertTrue("underscore after appname must not be part of the variable name",
                text.contains("${appname}_*.log"));
        Assert.assertFalse("must not copy keeper 45% off-heap Direct",
                text.contains("MaxDirectMemorySize=${MAX_DIRECT}g"));
    }

    @Test
    public void testPackageConfigHasAppIdAndNoCmsSecrets() throws IOException {
        Path config = packageRoot().resolve("src/main/config");
        String app = new String(Files.readAllBytes(config.resolve("app.properties")), StandardCharsets.UTF_8);
        Assert.assertTrue(app.contains("app.id=100077310"));
        String xpipe = new String(Files.readAllBytes(config.resolve("xpipe.properties")), StandardCharsets.UTF_8);
        Assert.assertFalse(xpipe.contains("comparator.cms.access.token"));
        Assert.assertFalse(xpipe.contains("comparator.cms.get.server.url"));
    }

    private static Path packageRoot() {
        return Paths.get("..", "package", "redis-comparator-package");
    }

    @Test
    public void testScheduledTasksUseScheduledExecutor() throws IOException {
        Path root = Paths.get("src/main/java");
        Assert.assertTrue(root.toFile().isDirectory());
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                String text;
                try {
                    text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new IllegalStateException(path.toString(), e);
                }
                boolean createsPool = text.contains("new ScheduledThreadPoolExecutor")
                        || text.contains("Executors.newScheduledThreadPool");
                String file = path.getFileName().toString();
                if (createsPool) {
                    Assert.assertTrue(path + " only Production may create comparatorTaskScheduled",
                            file.equals("Production.java") && text.contains("COMPARATOR_TASK_SCHEDULED"));
                } else if (text.contains("scheduleAtFixedRate") || text.contains("scheduleWithFixedDelay")) {
                    if (file.equals("ShardCompareTaskManager.java")) {
                        Assert.assertTrue(path + " must use dedicated TASK_SCHEDULED",
                                text.contains("TASK_SCHEDULED"));
                        Assert.assertFalse(path + " must not use shared SCHEDULED_EXECUTOR",
                                text.contains("SCHEDULED_EXECUTOR"));
                    } else {
                        Assert.assertTrue(path + " scheduled task must use SCHEDULED_EXECUTOR",
                                text.contains("SCHEDULED_EXECUTOR"));
                    }
                }
            });
        }
    }
}
