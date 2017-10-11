package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.service.impl.DcServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DcServiceTest extends AbstractConsoleTest {
	@Mock
	private DcTblDao mockedDcTblDao;
	@InjectMocks
	private DcServiceImpl dcService;

	@Test
	public void testLoad() {
		DcTbl target_result = new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC")
				.setDcLastModifiedTime("1234567");

		assertEquals(dcService.find("NTGXH").getId(), target_result.getId());
		assertEquals(dcService.find("NTGXH").getClusterName(), target_result.getClusterName());

	}

	@Before
	public void initMockData() throws DalException {
		when(mockedDcTblDao.findDcByDcName("NTGXH", DcTblEntity.READSET_FULL)).thenReturn(
				new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC").setDcLastModifiedTime("1234567"));
	}
}
