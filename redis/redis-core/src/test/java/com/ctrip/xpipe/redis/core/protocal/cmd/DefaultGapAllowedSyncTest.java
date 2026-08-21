package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultGapAllowedSyncTest extends AbstractRedisTest{
	
	private DefaultGapAllowedSync defaultGAsync;
	
	@Mock
	private ReplicationStoreManager replicationStoreManager;
	
	@Mock
	private ReplicationStore replicationStore;

	@Mock
	private MetaStore metaStore;

	private static final String REPL_ID = "0123456789012345678901234567890123456789";

	@Before
	public void beforeDefaultPsyncTest() throws Exception{
		when(replicationStoreManager.createIfNotExist()).thenReturn(replicationStore);
		when(replicationStore.getMetaStore()).thenReturn(metaStore);
		defaultGAsync = new DefaultGapAllowedSync(null, null, replicationStoreManager, scheduled);
	}

	/**
	 * T-S.1: ACTIVE full-sync replace must not bypass-close the old store when Manager.create fails.
	 */
	@Test
	public void testFullSyncReplaceDoesNotCloseOldStoreWhenCreateFails() throws Exception {
		when(replicationStoreManager.create()).thenThrow(new IOException("injected create fail"));
		try {
			defaultGAsync.doWhenFullSyncToNonFreshReplicationStore("new-repl-id");
			Assert.fail("expected create failure");
		} catch (XpipeRuntimeException e) {
			Assert.assertTrue(e.getCause() instanceof IOException);
		}
		verify(replicationStore, never()).close();
		verify(replicationStoreManager, times(1)).create();
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

	/**
	 * T-H3.CP8-proto: READING_COMMANDS must not swallow appendCommands IOException.
	 * Write fail → future failure; subsequent payload must not keep appending (not stay CONNECTED).
	 */
	@Test
	public void testAppendCommandsIoFailFailsFutureAndStopsReading() throws Exception {
		ReplStage psyncStage = mock(ReplStage.class);
		when(psyncStage.getProto()).thenReturn(ReplStage.ReplProto.PSYNC);
		when(metaStore.getCurrentReplStage()).thenReturn(psyncStage);
		when(metaStore.getCurReplStageReplId()).thenReturn(REPL_ID);
		when(replicationStore.getCurReplStageReplOff()).thenReturn(1000L);
		doThrow(new IOException("injected append fail")).when(replicationStore).appendCommands(any());

		defaultGAsync.getRequest();

		ByteBufReceiver.RECEIVER_RESULT continueResult = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer("+CONTINUE\r\n".getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, continueResult);
		Assert.assertFalse(defaultGAsync.future().isDone());

		ByteBufReceiver.RECEIVER_RESULT appendResult = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer("SET FOO BAR\r\n".getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.FAIL, appendResult);
		Assert.assertTrue(defaultGAsync.future().isDone());
		Assert.assertFalse(defaultGAsync.future().isSuccess());
		Assert.assertTrue(defaultGAsync.future().cause() instanceof IOException);
		Assert.assertTrue(defaultGAsync.future().cause().getMessage().contains("injected append fail"));
		verify(replicationStore, times(1)).appendCommands(any());

		ByteBufReceiver.RECEIVER_RESULT afterFail = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer("SET A B\r\n".getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.ALREADY_FINISH, afterFail);
		verify(replicationStore, times(1)).appendCommands(any());
		verify(replicationStoreManager, never()).create();
	}

	/**
	 * T-H3.CP5b: xsyncContinueFrom IO fail (after Helper retry) → setFailure disconnect.
	 * Must not send another PSYNC ? -1 on this command; must not retry commitContinueNewCmdThenMeta.
	 */
	@Test
	public void testXsyncContinueFromIoFailDisconnectsWithoutFullSync() throws Exception {
		when(metaStore.getCurrentReplStage()).thenReturn(null);
		when(replicationStore.isFresh()).thenReturn(true);
		doThrow(new IOException("injected xsyncContinueFrom fail"))
				.when(replicationStore).xsyncContinueFrom(anyString(), anyLong(), anyString(), any(), nullable(GtidSet.class));

		String request = defaultGAsync.getRequest().toString(Charset.defaultCharset());
		Assert.assertEquals("PSYNC ? -1\r\n", request);

		String xcontinue = "+" + GapAllowedSync.XPARTIAL_SYNC + " REPLID " + REPL_ID
				+ " REPLOFF 1234 GTID.SET A:1-10 MASTER.UUID A\r\n";
		ByteBufReceiver.RECEIVER_RESULT result = defaultGAsync.receive(null, Unpooled.wrappedBuffer(xcontinue.getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.FAIL, result);
		Assert.assertTrue(defaultGAsync.future().isDone());
		Assert.assertFalse(defaultGAsync.future().isSuccess());
		Assert.assertTrue(defaultGAsync.future().cause() instanceof IOException);
		Assert.assertTrue(defaultGAsync.future().cause().getMessage().contains("injected xsyncContinueFrom fail"));

		verify(replicationStore, times(1)).xsyncContinueFrom(anyString(), anyLong(), anyString(), any(), nullable(GtidSet.class));
		verify(replicationStore, never()).switchToXSync(anyString(), anyLong(), anyString(), any(), nullable(GtidSet.class));
		verify(replicationStore, never()).appendCommands(any());
		verify(replicationStoreManager, never()).create();

		ByteBufReceiver.RECEIVER_RESULT afterFail = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer("SET FOO BAR\r\n".getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.ALREADY_FINISH, afterFail);
		verify(replicationStore, never()).appendCommands(any());
	}

	/**
	 * T-H3.CP1.2: getGtidSet fail while assembling reconnect XSYNC must not become PSYNC ? -1
	 * or empty GTID. getRequest throws → AbstractCommand.setFailure → psyncFail reconnect.
	 */
	@Test
	public void testGetReplicationStoreSyncRequestGtidFailDoesNotSendFullSync() throws Exception {
		ReplStage xsyncStage = mock(ReplStage.class);
		when(xsyncStage.getProto()).thenReturn(ReplStage.ReplProto.XSYNC);
		when(metaStore.getCurrentReplStage()).thenReturn(xsyncStage);
		when(replicationStore.getGtidSet()).thenThrow(new XpipeRuntimeException("index reader error"));

		try {
			defaultGAsync.getRequest();
			Assert.fail("expected getGtidSet failure to propagate");
		} catch (XpipeRuntimeException e) {
			Assert.assertTrue(e.getMessage().contains("index reader error"));
		}
		verify(replicationStoreManager, never()).create();
	}

	/**
	 * T-H3.CP6.5: beginReadRdb must not swallow prepareRdb IOException.
	 * Fail → setFailure / dumpFail disconnect; must not keep reading RDB or append commands.
	 */
	@Test
	public void testPrepareRdbIoFailFailsFutureAndStopsReading() throws Exception {
		when(replicationStore.isFresh()).thenReturn(true);
		when(metaStore.getCurrentReplStage()).thenReturn(null);
		doThrow(new IOException("injected prepareRdb fail"))
				.when(replicationStore).prepareRdb(anyString(), anyLong(), any(), any(), nullable(GtidSet.class), nullable(String.class));

		String request = defaultGAsync.getRequest().toString(Charset.defaultCharset());
		Assert.assertEquals("PSYNC ? -1\r\n", request);

		ByteBufReceiver.RECEIVER_RESULT fullResult = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer(("+" + GapAllowedSync.FULL_SYNC + " " + REPL_ID + " 1000\r\n").getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, fullResult);
		Assert.assertFalse(defaultGAsync.future().isDone());

		ByteBufReceiver.RECEIVER_RESULT rdbHeader = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer("$6\r\n".getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.FAIL, rdbHeader);
		Assert.assertTrue(defaultGAsync.future().isDone());
		Assert.assertFalse(defaultGAsync.future().isSuccess());
		Assert.assertTrue(defaultGAsync.future().cause() instanceof RedisRuntimeException);
		Assert.assertTrue(defaultGAsync.future().cause().getCause() instanceof IOException);
		Assert.assertTrue(defaultGAsync.future().cause().getCause().getMessage().contains("injected prepareRdb fail"));

		verify(replicationStore, times(1)).prepareRdb(anyString(), anyLong(), any(), any(), nullable(GtidSet.class), nullable(String.class));
		verify(replicationStore, never()).appendCommands(any());
		verify(replicationStoreManager, never()).create();

		ByteBufReceiver.RECEIVER_RESULT afterFail = defaultGAsync.receive(null,
				Unpooled.wrappedBuffer("SET FOO BAR\r\n".getBytes()));
		Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.ALREADY_FINISH, afterFail);
		verify(replicationStore, never()).appendCommands(any());
	}

}
