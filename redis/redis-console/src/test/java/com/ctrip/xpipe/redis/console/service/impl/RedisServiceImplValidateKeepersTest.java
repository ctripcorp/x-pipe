package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisServiceImplValidateKeepersTest {

    @Spy
    @InjectMocks
    private RedisServiceImpl redisService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Mock
    private DalQueryHandler queryHandler;

    @Before
    public void setUp() {
        ReflectionTestUtils.setField(redisService, "queryHandler", queryHandler);
        doReturn(Collections.emptyList()).when(redisService).findAllByDcClusterShard(anyLong());
        when(queryHandler.handleQuery(any())).thenReturn(null);
    }

    @Test
    public void testValidateKeepersEmptyAllowed() {
        redisService.validateKeepers(Collections.emptyList());
    }

    @Test(expected = BadRequestException.class)
    public void testValidateKeepersSingleKeeperRejected() {
        redisService.validateKeepers(Lists.newArrayList(keeper(1, 10, "10.0.0.1")));
    }

    @Test(expected = BadRequestException.class)
    public void testValidateKeepersThreeTfsKeepersRejected() {
        mockTfsContainer(10, "10.0.0.1");
        mockTfsContainer(11, "10.0.0.2");
        mockTfsContainer(12, "10.0.0.3");
        redisService.validateKeepers(Lists.newArrayList(
                keeper(1, 10, "10.0.0.1"),
                keeper(2, 11, "10.0.0.2"),
                keeper(3, 12, "10.0.0.3")));
    }

    @Test
    public void testValidateKeepersOneBmTwoTfsAllowed() {
        mockBmContainer(10, "10.0.0.1");
        mockTfsContainer(11, "10.0.0.2");
        mockTfsContainer(12, "10.0.0.3");
        redisService.validateKeepers(Lists.newArrayList(
                keeper(1, 10, "10.0.0.1"),
                keeper(2, 11, "10.0.0.2"),
                keeper(3, 12, "10.0.0.3")));
    }

    @Test
    public void testValidateKeepersTwoBmAllowed() {
        mockBmContainer(10, "10.0.0.1");
        mockBmContainer(11, "10.0.0.2");
        redisService.validateKeepers(Lists.newArrayList(
                keeper(1, 10, "10.0.0.1"),
                keeper(2, 11, "10.0.0.2")));
    }

    private void mockBmContainer(long id, String ip) {
        when(keeperContainerService.find(id)).thenReturn(bmContainer(id, ip));
    }

    private void mockTfsContainer(long id, String ip) {
        when(keeperContainerService.find(id)).thenReturn(tfsContainer(id, ip));
    }

    private KeepercontainerTbl bmContainer(long id, String ip) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerDiskType("DEFAULT")
                .setKeepercontainerIp(ip);
    }

    private KeepercontainerTbl tfsContainer(long id, String ip) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerDiskType("tfs")
                .setKeepercontainerIp(ip);
    }

    private RedisTbl keeper(long id, long keeperContainerId, String ip) {
        return new RedisTbl()
                .setId(id)
                .setKeepercontainerId(keeperContainerId)
                .setRedisIp(ip)
                .setRedisPort(6380 + (int) id)
                .setDcClusterShardId(1L);
    }
}
