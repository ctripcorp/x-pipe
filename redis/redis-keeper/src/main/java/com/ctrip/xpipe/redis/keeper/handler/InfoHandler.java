package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 下午3:51:33
 */
public class InfoHandler extends AbstractCommandHandler{

	private Map<String, InfoSection> sections = Maps.newConcurrentMap();

	public InfoHandler() {
		register(new InfoAll());
		register(new InfoServer());
		register(new InfoReplication());
		register(new InfoStats());
		register(new InfoKeeper());
	}

	private void register(InfoSection section) {
		sections.put(section.name().toLowerCase().trim(), section);
	}

	@Override
	public String[] getCommands() {
		return new String[]{"info"};
	}

	@Override
	public boolean isLog(String[] args) {
		// INFO command is called by sentinel very frequently, so we need to hide the log
	    return false;
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));

		RedisKeeperServer redisKeeperServer = (RedisKeeperServer)redisClient.getRedisServer();
		String result;
		if(args.length == 0){
			result = new DefaultInfoSections().getInfo(redisKeeperServer);
		} else {
			result = doSectionHandler(args[0], redisKeeperServer);
		}
		redisClient.sendMessage(new CommandBulkStringParser(result).format());
	}

	private String doSectionHandler(String section, RedisKeeperServer redisKeeperServer) {
		return sections.get(section.toLowerCase().trim()).getInfo(redisKeeperServer);
	}

	private interface InfoSection {

		String getInfo(RedisKeeperServer keeperServer);

		String name();

	}

	private abstract class AbstractInfoSection implements InfoSection {

		protected String getHeader() {
			return String.format("# %s%s", name(), RedisProtocol.CRLF);
		}

		protected String strAndLong(String key, long val) {
			return String.format("%s:%d%s", key, val, RedisProtocol.CRLF);
		}

		protected String strAndFloat(String key, float val) {
			return String.format("%s:%f%s", key, val, RedisProtocol.CRLF);
		}

		protected String strAndNum(String key, int val) {
			return String.format("%s:%d%s", key, val, RedisProtocol.CRLF);
		}

		protected String strAndStr(String key, String val) {
			return String.format("%s:%s%s", key, val, RedisProtocol.CRLF);
		}
	}

	private class InfoAll extends AbstractInfoSection {

		@Override
		public String getInfo(RedisKeeperServer keeperServer) {
			StringBuilder sb = new StringBuilder();
			sb.append(sections.get("server").getInfo(keeperServer));
			sb.append(RedisProtocol.CRLF);
			sb.append(sections.get("replication").getInfo(keeperServer));
			sb.append(RedisProtocol.CRLF);
			sb.append(sections.get("stats").getInfo(keeperServer));
			return sb.toString();
		}

		@Override
		public String name() {
			return "All";
		}
	}

	private class DefaultInfoSections extends AbstractInfoSection {

		@Override
		public String getInfo(RedisKeeperServer keeperServer) {
			return sections.get("all").getInfo(keeperServer);
		}

		@Override
		public String name() {
			return null;
		}
	}

	private class InfoStats extends AbstractInfoSection {

		private static final String KEY_TOTAL_NET_INPUT_BYTES = "total_net_input_bytes";

		private static final String KEY_TOTAL_NET_OUTPUT_BYTES = "total_net_output_bytes";

		private static final String KEY_INSTANTANEOUS_INPUT_KBPS = "instantaneous_input_kbps";

		private static final String KEY_INSTANTANEOUS_OUTPUT_KBPS = "instantaneous_output_kbps";

		private static final String KEY_PSYNC_SEND_FAILURE = "psync_fail_send";

		private static final String KEY_LAST_FAIL_REASON = "last_psync_fail_reason";

		private static final String KEY_PEAK_INPUT_KBPS = "peak_input_kbps";

		private static final String KEY_PEAK_OUTPUT_KBPS = "peak_output_kbps";

		private static final String KEY_TOTAL_SYNC_FULL = "sync_full";

		private static final String KEY_TOTAL_SYNC_PARTIAL_OK = "sync_partial_ok";

		private static final String KEY_TOTAL_SYNC_PARTIAL_ERROR = "sync_partial_err";

		@Override
		public String getInfo(RedisKeeperServer keeperServer) {
			long kilo = 1024;
			KeeperStats stats = keeperServer.getKeeperMonitor().getKeeperStats();
			StringBuilder sb = new StringBuilder();
			sb.append(getHeader());
			sb.append(strAndLong(KEY_TOTAL_SYNC_FULL, stats.getFullSyncCount()));
			sb.append(strAndLong(KEY_TOTAL_SYNC_PARTIAL_OK, stats.getPartialSyncCount()));
			sb.append(strAndLong(KEY_TOTAL_SYNC_PARTIAL_ERROR, stats.getPartialSyncErrorCount()));
			sb.append(strAndLong(KEY_TOTAL_NET_INPUT_BYTES, stats.getInputBytes()));
			sb.append(strAndLong(KEY_TOTAL_NET_OUTPUT_BYTES, stats.getOutputBytes()));
			sb.append(strAndFloat(KEY_INSTANTANEOUS_INPUT_KBPS, ((float)stats.getInputInstantaneousBPS() / kilo)));
			sb.append(strAndFloat(KEY_INSTANTANEOUS_OUTPUT_KBPS, ((float)stats.getOutputInstantaneousBPS() / kilo)));
			sb.append(strAndLong(KEY_PEAK_INPUT_KBPS, stats.getPeakInputInstantaneousBPS() / kilo));
			sb.append(strAndLong(KEY_PEAK_OUTPUT_KBPS, stats.getPeakOutputInstantaneousBPS() / kilo));
			sb.append(strAndLong(KEY_PSYNC_SEND_FAILURE, stats.getPsyncSendFailCount()));
			if(stats.getLastPsyncFailReason() != null) {
				sb.append(strAndStr(KEY_LAST_FAIL_REASON, stats.getLastPsyncFailReason().name()));
			}
			return sb.toString();
		}

		@Override
		public String name() {
			return "Stats";
		}
	}

	private class InfoServer extends AbstractInfoSection {

		@Override
		public String getInfo(RedisKeeperServer keeperServer) {
			StringBuilder sb = new StringBuilder();
			sb.append(getHeader());
			sb.append(keeperServer.info() + RedisProtocol.CRLF);
			return sb.toString();
		}

		@Override
		public String name() {
			return "Server";
		}
	}

	//keeper has it while redis not
	private class InfoKeeper extends AbstractInfoSection{

		@Override
		public String getInfo(RedisKeeperServer keeperServer) {

			StringBuilder sb = new StringBuilder();

			MasterStats masterStats = keeperServer.getKeeperMonitor().getMasterStats();
			sb.append("master:" + RedisProtocol.CRLF);
			sb.append("commands_instantaneous_ops_per_sec:" + masterStats.getCommandBPS() + RedisProtocol.CRLF);
			sb.append("commands_total_length:" + masterStats.getCommandTotalLength() + RedisProtocol.CRLF);
			sb.append("last_master_type:" + masterStats.lastMasterType() + RedisProtocol.CRLF);


			ReplicationStoreStats replicationStoreStats = keeperServer.getKeeperMonitor().getReplicationStoreStats();
			sb.append("last_repl_down_time:" + DateTimeUtils.timeAsString(replicationStoreStats.getLastReplDownTime()) + RedisProtocol.CRLF);
			return sb.toString();
		}

		@Override
		public String name() {
			return "keeper";
		}
	}

	private class InfoReplication extends AbstractInfoSection {

		@Override
		public String getInfo(RedisKeeperServer redisKeeperServer) {
			StringBuilder sb = new StringBuilder();
			ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
			long slaveReplOffset = replicationStore.getEndOffset();
			KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();
			sb.append(getHeader());
			sb.append("role:" + Server.SERVER_ROLE.SLAVE + RedisProtocol.CRLF);
			sb.append(RedisProtocol.KEEPER_ROLE_PREFIX + ":" + redisKeeperServer.role() + RedisProtocol.CRLF);
			sb.append("state:" + redisKeeperServer.getRedisKeeperServerState().keeperState() + RedisProtocol.CRLF);
			RedisMaster redisMaster =  redisKeeperServer.getRedisMaster();
			String masterHost = redisMaster == null ? null: redisMaster.masterEndPoint().getHost();
			Integer masterPort = redisMaster == null ? null: redisMaster.masterEndPoint().getPort();
			if(masterHost != null){
				sb.append("master_host:" + masterHost + RedisProtocol.CRLF );
				sb.append("master_port:"  + masterPort +  RedisProtocol.CRLF );
				/**
				 * If not report master link status as up, then sentinal is found crashed
				 * when sentinel is doing slaveof new_master_ip new_master_port
				 */
				sb.append("master_link_status:up" +  RedisProtocol.CRLF );
			}
			/**
			 * To make sure keeper is the least option to be the new master when master is down
			 */
			sb.append("slave_repl_offset:" + slaveReplOffset + RedisProtocol.CRLF);
			sb.append("slave_priority:0" + RedisProtocol.CRLF);

			Set<RedisSlave<RedisKeeperServer>> slaves = redisKeeperServer.slaves();
			sb.append("connected_slaves:" + slaves.size() + RedisProtocol.CRLF);
			int slaveIndex = 0;
			for(RedisSlave slave : slaves){
				sb.append(String.format("slave%d:%s" + RedisProtocol.CRLF, slaveIndex, slave.info()));
				slaveIndex++;
			}

			long beginOffset = keeperRepl.getBeginOffset();
			MetaStore metaStore = replicationStore.getMetaStore();
			String replid = metaStore == null? ReplicationStoreMeta.EMPTY_REPL_ID : metaStore.getReplId();
			String replid2 = metaStore == null? ReplicationStoreMeta.EMPTY_REPL_ID : metaStore.getReplId2();
			long  secondReplIdOffset = metaStore == null? ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET : metaStore.getSecondReplIdOffset();

			if(replid == null){
				replid = ReplicationStoreMeta.EMPTY_REPL_ID;
			}
			if(replid2 == null){
				replid2 = ReplicationStoreMeta.EMPTY_REPL_ID;
			}

			sb.append("master_replid:" + replid + RedisProtocol.CRLF);
			sb.append("master_replid2:" + replid2 + RedisProtocol.CRLF);
			sb.append("master_repl_offset:" + keeperRepl.getEndOffset() + RedisProtocol.CRLF);
			sb.append("second_repl_offset:" + secondReplIdOffset + RedisProtocol.CRLF);

			sb.append("repl_backlog_active:1" + RedisProtocol.CRLF);
			sb.append("repl_backlog_first_byte_offset:" + beginOffset+ RedisProtocol.CRLF);
			try {
				long endOffset = keeperRepl.getEndOffset();
				sb.append("master_repl_offset:" + endOffset + RedisProtocol.CRLF);
				sb.append("repl_backlog_size:" + (endOffset - beginOffset + 1) + RedisProtocol.CRLF);
				sb.append("repl_backlog_histlen:" + (endOffset - beginOffset + 1)+ RedisProtocol.CRLF);
			} catch (Throwable ex) {
				sb.append("error_message:" + ex.getMessage() + RedisProtocol.CRLF);
				logger.info("Cannot calculate end offset", ex);
			}
			return sb.toString();
		}

		@Override
		public String name() {
			return "Replication";
		}
	}

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}
}
