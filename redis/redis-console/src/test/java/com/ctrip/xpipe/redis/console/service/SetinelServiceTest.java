package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.SentinelGroupTblDao;
import com.ctrip.xpipe.redis.console.service.impl.SentinelGroupServiceImpl;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class SetinelServiceTest extends AbstractConsoleTest {
	@Mock
	private SentinelGroupTblDao mockedSetinelTblDao;
	@InjectMocks
	private SentinelGroupServiceImpl setinelService;

//	@Test
//	public void testMetasService() {
//		SetinelTbl target_setinel = new SetinelTbl().setSetinelId(1).setSetinelAddress("11111");
//
//		assertEquals(setinelService.findAllByDcName("NTGXH").get(0).getSetinelAddress(),
//				target_setinel.getSetinelAddress());
//	}
//
//	@Before
//	public void initMockData() throws Exception {
//		when(mockedSetinelTblDao.findByDcName("NTGXH", SetinelTblEntity.READSET_FULL))
//				.thenReturn(Arrays.asList(new SetinelTbl().setSetinelId(1).setSetinelAddress("11111")));
//	}
}
