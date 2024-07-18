package com.ctrip.xpipe.redis.console.repository;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 2024/7/17
 */
public class MigrationBiClusterRepositoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationBiClusterRepository migrationBiClusterRepository;

    @Test
    public void testInsertAndQuery() {
        List<MigrationBiClusterEntity> entities = new ArrayList<>();
        IntStream.range(0, 30).forEach(i -> {
            MigrationBiClusterEntity entity = new MigrationBiClusterEntity();
            entity.setClusterId((long)i);
            entity.setOperationTime(new Date());
            entity.setOperator("test");
            entity.setPublishInfo("{\"ssss\":\"qqqqq\"}");
            entity.setStatus("SUCCESS");
            entities.add(entity);
        });
        migrationBiClusterRepository.batchInsert(entities);

        List<MigrationBiClusterEntity> record = migrationBiClusterRepository.selectAll();
        logger.info("[testInsertAndQuery] {}", record);
    }

}
