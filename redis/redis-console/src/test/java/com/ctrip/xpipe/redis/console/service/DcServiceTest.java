package com.ctrip.xpipe.redis.console.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;

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
	private DcService dcService;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);

		try {
			generateDcMockData();
		} catch (Exception e) {
			logger.error("Generate Dc mock data failed.", e);
		}
	}

	@Test
	public void testLoad() {
		DcTbl target_result = new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC")
				.setDcLastModifiedTime("1234567");

		assertEquals(dcService.load("NTGXH").getId(), target_result.getId());
		assertEquals(dcService.load("NTGXH").getClusterName(), target_result.getClusterName());

	}

	private void generateDcMockData() throws DalException {
		when(mockedDcTblDao.findDcByDcName("NTGXH", DcTblEntity.READSET_FULL)).thenReturn(
				new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC").setDcLastModifiedTime("1234567"));
	}
}
