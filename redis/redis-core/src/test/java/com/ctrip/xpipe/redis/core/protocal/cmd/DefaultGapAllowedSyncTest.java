package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.Charset;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultGapAllowedSyncTest extends AbstractRedisTest{
	
	private DefaultGapAllowedSync defaultGAsync;
	
	@Mock
	private ReplicationStoreManager replicationStoreManager;
	
	@Mock
	private ReplicationStore replicationStore;

	@Before
	public void beforeDefaultPsyncTest() throws Exception{
		when(replicationStoreManager.createIfNotExist()).thenReturn(replicationStore);
		defaultGAsync = new DefaultGapAllowedSync(null, null, replicationStoreManager, scheduled);
	}

	@Test
	public void testFormatSyncRequest() {
		AbstractGapAllowedSync.PsyncRequest psync = new AbstractGapAllowedSync.PsyncRequest();
		psync.setReplId("MY_REPL_ID");
		String hello = psync.format().toString(Charset.defaultCharset());
		Assert.assertEquals(hello, "PSYNC MY_REPL_ID -1\r\n");
		psync.setReplOff(1234);
		Assert.assertEquals(psync.format().toString(Charset.defaultCharset()), "PSYNC MY_REPL_ID 1234\r\n");

		AbstractGapAllowedSync.XsyncRequest xsync = new AbstractGapAllowedSync.XsyncRequest();
		xsync.setUuidIntrested("*");
		xsync.setGtidSet(new GtidSet("A:1,B:2"));
		xsync.setLost(new GtidSet("C:1-5"));
		xsync.setMaxGap(1000);
		Assert.assertEquals(xsync.format().toString(Charset.defaultCharset()), "XSYNC * A:1,B:2 MAXGAP 1000 GTID.LOST C:1-5\r\n");
	}

	@Test
	public void testParseSyncReply() {
		AbstractGapAllowedSync.SyncReply reply;

		Assertions.assertThrows(RedisRuntimeException.class, () -> defaultGAsync.parseSyncReply("foo bar"));

		reply = defaultGAsync.parseSyncReply("FULLRESYNC 0123456789012345678901234567890123456789 1000");
		Assert.assertTrue(reply instanceof AbstractGapAllowedSync.FullresyncReply);
		Assert.assertEquals(reply.getReplId(), "0123456789012345678901234567890123456789");
		Assert.assertEquals(reply.getReplOff(), 1000);

		reply = defaultGAsync.parseSyncReply("CONTINUE");
		Assert.assertTrue(reply instanceof AbstractGapAllowedSync.ContinueReply);
		Assert.assertEquals(reply.getReplId(), null);
		Assert.assertEquals(reply.getReplOff(), -1);

		reply = defaultGAsync.parseSyncReply("CONTINUE 0123456789012345678901234567890123456789");
		Assert.assertTrue(reply instanceof AbstractGapAllowedSync.ContinueReply);
		Assert.assertEquals(reply.getReplId(), "0123456789012345678901234567890123456789");
		Assert.assertEquals(reply.getReplOff(), -1);

		reply = defaultGAsync.parseSyncReply("CONTINUE 0123456789012345678901234567890123456789 1234");
		Assert.assertTrue(reply instanceof AbstractGapAllowedSync.ContinueReply);
		Assert.assertEquals(reply.getReplId(), "0123456789012345678901234567890123456789");
		Assert.assertEquals(reply.getReplOff(), 1234);

		Assertions.assertThrows(RedisRuntimeException.class, () -> defaultGAsync.parseSyncReply("XFULLRESYNC"));
		Assertions.assertThrows(RedisRuntimeException.class, () -> defaultGAsync.parseSyncReply("XFULLRESYNC GTID.LOST \"\" MASTER.UUID master-uuid"));

		reply = defaultGAsync.parseSyncReply("XFULLRESYNC GTID.LOST \"\" MASTER.UUID master-uuid REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 FOO BAR");
		Assert.assertTrue(reply instanceof AbstractGapAllowedSync.XFullresyncReply);
		Assert.assertEquals(reply.getReplId(), "0123456789012345678901234567890123456789");
		Assert.assertEquals(reply.getReplOff(), 1234);
		Assert.assertEquals(((AbstractGapAllowedSync.XFullresyncReply) reply).getGtidLost(), new GtidSet(GtidSet.EMPTY_GTIDSET));

		Assertions.assertThrows(RedisRuntimeException.class, () -> defaultGAsync.parseSyncReply("XCONTINUE"));
		Assertions.assertThrows(RedisRuntimeException.class, () -> defaultGAsync.parseSyncReply("XCONTINUE GTID.SET A:1,B:2 MASTER.UUID A"));

		reply = defaultGAsync.parseSyncReply("XCONTINUE REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 GTID.SET A:1,B:2 MASTER.UUID A FOO BAR");
		Assert.assertTrue(reply instanceof AbstractGapAllowedSync.XContinueReply);
		Assert.assertEquals(reply.getReplId(), "0123456789012345678901234567890123456789");
		Assert.assertEquals(reply.getReplOff(), 1234);
		Assert.assertEquals(((AbstractGapAllowedSync.XContinueReply) reply).getGtidCont(), new GtidSet("A:1,B:2"));
	}

}
