package com.ctrip.xpipe.redis.integratedtest.full.multidc.manul;


import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.integratedtest.full.multidc.AbstractMultiDcTest;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class ManualDcStarter extends AbstractMultiDcTest{
	
	@Test
	public void startConsole() throws Exception{
	
		//clean environment
		stopXippe();
		FileUtils.forceDelete(new File(new File(getTestFileDir()).getParent()));
		FileUtils.forceMkdir(new File(getTestFileDir()));
		
		startConsoleServer();
	}
	

	@Test
	public void startDc0() throws Exception{
		
		startDc(getAndSetDc(0));
	}

	@Test
	public void stopDc0() throws ExecuteException, IOException{
		
		DcMeta dcMeta = getDcMeta(getAndSetDc(0));
		stopDc(dcMeta);
	}

	/////////////////////////////////////////DC1/////////////////////////////////////////

	@Test
	public void startDc1() throws Exception{
		
		startDc(getAndSetDc(1));
	}

	@Test
	public void stopDc1() throws ExecuteException, IOException{
		
		DcMeta dcMeta = getDcMeta(getAndSetDc(1));
		stopDc(dcMeta);
	}

	@Override
	protected boolean startAllDc() {
		return false;
	}
	
	@Override
	protected boolean deleteTestDirBeforeTest() {
		return false;
	}
	
	@Override
	protected boolean stopIntegratedServers() {
		return false;
	}
	
	@Override
	protected boolean staticPort() {
		return true;
	}


	@After
	public void afterDcStarter() throws IOException{
		
		waitForAnyKeyToExit();
	}
	
}
