package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.database.ConnectionPoolDesc;
import com.ctrip.xpipe.database.ConnectionPoolHolder;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ResourceInfo;
import com.ctrip.xpipe.redis.console.ds.XPipeDataSource;
import com.ctrip.xpipe.spring.AbstractController;
import jakarta.annotation.Resource;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Stream;

import static com.ctrip.xpipe.redis.console.migration.MigrationResources.*;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.tryGetTomcatHttpExecutor;

/**
 * @author lishanglin
 * date 2022/3/31
 */
@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ResourceController extends AbstractController {

    @Resource(name = MIGRATION_EXECUTOR)
    private ExecutorService migrationExecutor;

    @Resource(name = MIGRATION_PREPARE_EXECUTOR)
    private ExecutorService prepareExecutor;

    @Resource(name = MIGRATION_IO_CALLBACK_EXECUTOR)
    private ExecutorService ioCallBackExecutor;

    private DataSource dataSource;

    private static final String CLAZZ_DELEGATED_EXECUTOR_SERVICE = "DelegatedExecutorService";

    @GetMapping("/resource")
    public ResourceInfo getResource() {
        ResourceInfo resourceInfo = new ResourceInfo();
        ThreadPoolExecutor migrationThreadPoolExecutor = getInnerPoolExecutor(migrationExecutor);
        ThreadPoolExecutor prepareThreadPoolExecutor = getInnerPoolExecutor(prepareExecutor);
        ThreadPoolExecutor ioCallBackThreadPoolExecutor = getInnerPoolExecutor(ioCallBackExecutor);

        resourceInfo.collectDataFromMigrationExecutor(migrationThreadPoolExecutor);
        resourceInfo.collectDataFromPrepareExecutor(prepareThreadPoolExecutor);
        resourceInfo.collectDataFromIoCallbackExecutor(ioCallBackThreadPoolExecutor);

        Executor tomcatHttpExecutor = tryGetTomcatHttpExecutor();
        if (tomcatHttpExecutor instanceof ThreadPoolExecutor)
            resourceInfo.collectDataFromHttpExecutor((ThreadPoolExecutor) tomcatHttpExecutor);

        resourceInfo.connectionPoolDesc = tryGetConnectionPoolDesc();
        return resourceInfo;
    }

    private ThreadPoolExecutor getInnerPoolExecutor(ExecutorService executor) {
        Class<?>[] classes = Executors.class.getDeclaredClasses();
        Class<?> classDelegatedExecutorService = Stream.of(classes)
                .filter(clazz -> clazz.getSimpleName().equals(CLAZZ_DELEGATED_EXECUTOR_SERVICE))
                .findFirst().orElse(null);

        if (null == classDelegatedExecutorService) {
            logger.info("[getInnerPoolExecutor][reflect not found] {}", CLAZZ_DELEGATED_EXECUTOR_SERVICE);
            return null;
        }

        try {
            Field innerExecutorField = classDelegatedExecutorService.getDeclaredField("e");
            innerExecutorField.setAccessible(true);
            Object obj = innerExecutorField.get(executor);
            if (obj instanceof ThreadPoolExecutor) {
                return (ThreadPoolExecutor) obj;
            } else if(null != obj) {
                logger.info("[getInnerPoolExecutor][unexpected executor] {}", obj.getClass().getName());
            } else {
                logger.info("[getInnerPoolExecutor][executor null]");
            }
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            logger.info("[getInnerPoolExecutor][reflect no field]", e);
        }

        return null;
    }

    private ConnectionPoolDesc tryGetConnectionPoolDesc() {
        DataSource localDataSource = tryGetDataSource();

        if (!(localDataSource instanceof XPipeDataSource)) return null;

        if (((XPipeDataSource)localDataSource).getInnerDataSource() instanceof ConnectionPoolHolder) {
            return ((ConnectionPoolHolder) ((XPipeDataSource)localDataSource).getInnerDataSource()).getConnectionPoolDesc();
        }

        return null;
    }

    private DataSource tryGetDataSource() {
        if (null != dataSource) return dataSource;
        synchronized (this) {
            if (null != dataSource) return dataSource;
            try {
                DataSourceManager dataSourceManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
                if (dataSourceManager.getDataSourceNames().isEmpty()) {
                    logger.info("[tryGetDataSource] no datasource found");
                } else {
                    dataSource = dataSourceManager.getDataSource(dataSourceManager.getDataSourceNames().get(0));
                }
            } catch (ComponentLookupException e) {
                logger.info("[tryGetDataSource] xpipe datasource miss");
            } catch (Throwable th) {
                logger.info("[tryGetDataSource] fail", th);
            }

            return dataSource;
        }
    }

}
