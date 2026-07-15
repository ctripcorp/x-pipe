package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupEntity;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.repository.AzGroupMappingRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupRepository;
import com.ctrip.xpipe.redis.console.repository.DcRepository;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.Mockito.when;

public class AzGroupCacheImplTest {

    @Mock
    private ConsoleConfig config;

    @Mock
    private DcRepository dcRepository;

    @Mock
    private AzGroupRepository azGroupRepository;

    @Mock
    private AzGroupMappingRepository azGroupMappingRepository;

    private AzGroupCacheImpl cache;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        cache = new AzGroupCacheImpl(config, dcRepository, azGroupRepository, azGroupMappingRepository);
    }

    // az_group id=9 (SGP) 有 mapping，az_group id=10 (SGP-NOR) 也有 mapping
    @Test
    public void testGetAzGroupById_returnCorrectGroup() {
        AzGroupEntity group9 = new AzGroupEntity().setId(9L).setName("SGP").setRegion("SGP");
        AzGroupEntity group10 = new AzGroupEntity().setId(10L).setName("SGP-NOR").setRegion("SGP");

        Map<Long, List<Long>> mappings = new HashMap<>();
        mappings.put(9L, Arrays.asList(17L, 20L));
        mappings.put(10L, Collections.singletonList(20L));

        Map<Long, String> dcNames = new HashMap<>();
        dcNames.put(17L, "SGP-ALI");
        dcNames.put(20L, "SGP-NOR");

        when(azGroupRepository.selectAll()).thenReturn(Arrays.asList(group9, group10));
        when(azGroupMappingRepository.getAzGroupAzsMap()).thenReturn(mappings);
        when(dcRepository.getDcIdNameMap()).thenReturn(dcNames);

        AzGroupModel result = cache.getAzGroupById(9L);

        Assert.assertNotNull(result);
        Assert.assertEquals(9L, (long) result.getId());
        Assert.assertEquals("SGP", result.getName());
        Assert.assertTrue(result.getAzs().contains("SGP-ALI"));
        Assert.assertTrue(result.getAzs().contains("SGP-NOR"));
    }

    // az_group 在 mapping 表中没有记录时，不应该 NPE，也不应该加载进缓存
    @Test
    public void testLoadCache_azGroupWithoutMapping_doesNotThrow() {
        AzGroupEntity groupNoMapping = new AzGroupEntity().setId(9L).setName("SGP").setRegion("SGP");

        when(azGroupRepository.selectAll()).thenReturn(Collections.singletonList(groupNoMapping));
        when(azGroupMappingRepository.getAzGroupAzsMap()).thenReturn(Collections.emptyMap());
        when(dcRepository.getDcIdNameMap()).thenReturn(Collections.emptyMap());

        // 不应抛异常
        List<AzGroupModel> result = cache.getAllAzGroup();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    // repository 抛异常时，缓存不应崩溃，调用方不应 NPE
    @Test
    public void testLoadCache_repositoryThrows_cacheRemainsNull() {
        when(azGroupRepository.selectAll()).thenThrow(new RuntimeException("db error"));

        // 首次 getAllAzGroup 触发 load，异常被 catch，azGroupModels 仍为 null
        List<AzGroupModel> result = cache.getAllAzGroup();
        Assert.assertNull(result);
    }

}
