package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
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
 * 周期任务入口挂 {@code SCHEDULED_EXECUTOR}，不另起比对线程池。
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

        Assert.assertNotNull(getBean(XpipeNettyClientKeyedObjectPool.class));
        Assert.assertNotNull(AbstractSpringConfigContext.getApplicationContext()
                .getBean(AbstractSpringConfigContext.SCHEDULED_EXECUTOR, ScheduledExecutorService.class));
        Assert.assertNotNull(AbstractSpringConfigContext.getApplicationContext()
                .getBean(AbstractSpringConfigContext.GLOBAL_EXECUTOR, ExecutorService.class));

        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(Production.class).size());
        Assert.assertEquals(0, AbstractSpringConfigContext.getApplicationContext()
                .getBeansOfType(org.springframework.web.client.RestTemplate.class).size());
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
                Assert.assertFalse(path + " must not create a private scheduled pool",
                        text.contains("new ScheduledThreadPoolExecutor")
                                || text.contains("Executors.newScheduledThreadPool"));
                if (text.contains("scheduleAtFixedRate") || text.contains("scheduleWithFixedDelay")) {
                    Assert.assertTrue(path + " scheduled task must use SCHEDULED_EXECUTOR",
                            text.contains("SCHEDULED_EXECUTOR"));
                }
            });
        }
    }
}
