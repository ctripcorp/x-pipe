package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.google.common.collect.Lists;
import org.junit.Assert;
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

    /**
     * 容器 keeper 的 redis_ip 与 keepercontainer_ip 都是占位值 0.0.0.0，比 IP 恒成立，
     * 身份校验必须改比 hostname 才有意义。
     */
    @Test
    public void testValidatePodKeepersMatchByHostName() {
        mockPodContainer(10, "pod-a-0");
        mockPodContainer(11, "pod-b-0");
        redisService.validateKeepers(Lists.newArrayList(
                podKeeper(1, 10, "pod-a-0"),
                podKeeper(2, 11, "pod-b-0")));
    }

    @Test
    public void testValidatePodKeeperRejectsHostNameMismatch() {
        mockPodContainer(10, "pod-a-0");
        mockPodContainer(11, "pod-b-0");
        try {
            redisService.validateKeepers(Lists.newArrayList(
                    // keeper 声称在 pod-a-0，但其 keepercontainer 是 pod-b-0
                    podKeeper(1, 11, "pod-a-0"),
                    podKeeper(2, 10, "pod-b-0")));
            Assert.fail("should reject hostname mismatch");
        } catch (BadRequestException e) {
            // 断言到具体原因，避免测试因其他校验（端口占用、同容器重复）而误报通过
            Assert.assertTrue("unexpected reason: " + e.getMessage(),
                    e.getMessage().contains("hostname"));
        }
    }

    /**
     * 混布：物理机 keeper 仍按 IP 校验，容器 keeper 按 hostname，同一批里两种可以并存。
     */
    @Test
    public void testValidateMixedPhysicalAndPodKeepers() {
        mockBmContainer(10, "10.0.0.1");
        mockPodContainer(11, "pod-b-0");
        redisService.validateKeepers(Lists.newArrayList(
                keeper(1, 10, "10.0.0.1"),
                podKeeper(2, 11, "pod-b-0")));
    }

    @Test
    public void testValidatePhysicalKeeperStillRejectsIpMismatch() {
        mockBmContainer(10, "10.0.0.1");
        mockBmContainer(11, "10.0.0.2");
        try {
            redisService.validateKeepers(Lists.newArrayList(
                    keeper(1, 10, "10.0.0.9"),
                    keeper(2, 11, "10.0.0.2")));
            Assert.fail("should reject ip mismatch");
        } catch (BadRequestException e) {
            Assert.assertTrue("unexpected reason: " + e.getMessage(),
                    e.getMessage().contains("ip"));
        }
    }

    private void mockPodContainer(long id, String hostName) {
        when(keeperContainerService.find(id)).thenReturn(new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerDiskType("DEFAULT")
                .setKeepercontainerIp("0.0.0.0")
                .setKeepercontainerHostName(hostName));
    }

    private RedisTbl podKeeper(long id, long keeperContainerId, String hostName) {
        return new RedisTbl()
                .setId(id)
                .setKeepercontainerId(keeperContainerId)
                .setRedisIp("0.0.0.0")
                .setHostName(hostName)
                .setRedisPort(6380 + (int) id)
                .setDcClusterShardId(1L);
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
                .setHostName("")
                .setRedisPort(6380 + (int) id)
                .setDcClusterShardId(1L);
    }
}
