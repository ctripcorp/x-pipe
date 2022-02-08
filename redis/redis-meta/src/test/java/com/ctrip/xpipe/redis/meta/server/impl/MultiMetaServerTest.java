package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public class MultiMetaServerTest extends AbstractMetaServerTest{
	
	@Test
	public void testMultiProxy(){
		
		int serversCount = 10;
		List<MetaServer> servers = new LinkedList<>();
		
		for(int i=0; i < serversCount - 1 ; i++){
			servers.add(mock(MetaServer.class));
		}
		
		MetaServer metaServer = MultiMetaServer.newProxy(mock(MetaServer.class), servers);
		
		final ForwardInfo forwardInfo = new ForwardInfo();
		
		metaServer.clusterDeleted(getClusterId(), forwardInfo);
		
		for(MetaServer mockServer :  servers){
			verify(mockServer).clusterDeleted(eq(getClusterId()),  argThat(new ArgumentMatcher<ForwardInfo>() {

				@Override
				public boolean matches(ForwardInfo item) {
					//should be cloned
					if(item == forwardInfo){
						return false;
					}
					return true;
				}
			}));
		}
		
	}

}
