package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public abstract class AbstractSentinelCommand<T> extends AbstractRedisCommand<T>{
	
	public static String SENTINEL = "sentinel";

	public AbstractSentinelCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		setInOutPayloadFactory(new InOutPayloadFactory.DirectByteBufInOutPayloadFactory());
	}

	public AbstractSentinelCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
		super(clientPool, scheduled, commandTimeoutMilli);
		setInOutPayloadFactory(new InOutPayloadFactory.DirectByteBufInOutPayloadFactory());
	}

	public static class Sentinels extends AbstractSentinelCommand<List<Sentinel>>{

		private static final Logger logger = LoggerFactory.getLogger(Sentinels.class);

		public static String SENTINELS = "sentinels";
		
		private String masterName;

		public Sentinels(SimpleObjectPool<NettyClient> clientPool, String masterName, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.masterName = masterName;
		}

		public Sentinels(SimpleObjectPool<NettyClient> clientPool, String masterName, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
			this.masterName = masterName;
		}

		@Override
		protected List<Sentinel> format(Object payload) {
			
			if(!(payload instanceof Object[])){
				throw new IllegalStateException("expected Object[], but:" + payload + "," + payload.getClass());
			}
			return doFormat((Object[])payload);			
		}

		private List<Sentinel> doFormat(Object[] payload) {
			
			List<Sentinel> sentinels = new LinkedList<>();
			for(Object sentinel : payload){
				if(!(sentinel instanceof Object[])){
					throw new IllegalStateException("expected Object[], but:" + sentinel + "," + sentinel.getClass());
				}
				sentinels.add(objectToSentinel((Object[])sentinel));
			}
			return sentinels;
		}

		private Sentinel objectToSentinel(Object[] sentinel) {
			
			if(sentinel.length < 6){
				throw new IllegalStateException("expected arg len >=6, but:" + sentinel.length + "," + StringUtil.join(",", sentinel));
			}
			return new Sentinel(payloadToString(sentinel[1]), 
					payloadToString(sentinel[3]), payloadToInteger(sentinel[5]));
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, SENTINELS, masterName).format();
		}

		@Override
		public String toString() {
			return String.format("%s %s %s", SENTINEL, SENTINELS, masterName);
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}
	
	
	public static class SentinelAdd extends AbstractSentinelCommand<String> {

		private static final Logger logger = LoggerFactory.getLogger(SentinelAdd.class);
		
		public static String MONITOR = "monitor";
		
		private String masterIp;
		private int masterPort;
		private int quorum;
		private String masterName;
		
		public SentinelAdd(SimpleObjectPool<NettyClient> clientPool, String masterName, String masterIp, int masterPort, int quorum, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.masterIp = masterIp;
			this.masterPort = masterPort;
			this.quorum = quorum;
			this.masterName = masterName;
		}

		public SentinelAdd(SimpleObjectPool<NettyClient> clientPool, String masterName, String masterIp, int masterPort, int quorum, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
			this.masterIp = masterIp;
			this.masterPort = masterPort;
			this.quorum = quorum;
			this.masterName = masterName;
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, MONITOR, masterName, masterIp, String.valueOf(masterPort), String.valueOf(quorum)).format();
		}
		
		@Override
		public String toString() {
			return String.format("%s %s %s %s %d %d", SENTINEL, MONITOR, masterName, masterIp, masterPort, quorum);
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}
	
	public static class SentinelRemove extends AbstractSentinelCommand<String>{

		private static final Logger logger = LoggerFactory.getLogger(SentinelRemove.class);

		public static String REMOVE = "remove";
		
		private String masterName;

		public SentinelRemove(SimpleObjectPool<NettyClient> clientPool, String masterName, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.masterName = masterName;
		}

		public SentinelRemove(SimpleObjectPool<NettyClient> clientPool, String masterName, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
			this.masterName = masterName;
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, REMOVE, masterName).format();
		}
		
		@Override
		public String toString() {
			return String.format("%s %s %s", SENTINEL, REMOVE, masterName);
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}

	public static class SentinelMaster extends AbstractSentinelCommand<HostPort> {

		private static final Logger logger = LoggerFactory.getLogger(SentinelMaster.class);

		private static final String MASTER = "master";

		private String monitorName;

		public SentinelMaster(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
							  String monitorName) {

			super(clientPool, scheduled);
			this.monitorName = monitorName;
		}

		@Override
		public HostPort format(Object payload) {
			if(!(payload instanceof Object[])){
				throw new IllegalStateException("expected Object[], but:" + payload + "," + payload.getClass());
			}
			return doFormat((Object[])payload);
		}

		private HostPort doFormat(Object[] payload) {
			if(payload.length < 6) {
				throw new IllegalStateException("expected arg len >=6, but:" + payload.length + ","
						+ StringUtil.join(",", payload));
			}
			String monitorNameFromSentinel = payloadToString(payload[1]);
			if(!this.monitorName.equalsIgnoreCase(monitorNameFromSentinel)) {
				throw new IllegalMonitorStateException("expected monitor name: " + monitorName + ", but is: "
						+ monitorNameFromSentinel);
			}
			return new HostPort(payloadToString(payload[3]), payloadToInteger(payload[5]));
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, MASTER, monitorName).format();
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}

	public static class SentinelSlaves extends AbstractSentinelCommand<List<HostPort>> {

		private static final Logger logger = LoggerFactory.getLogger(SentinelSlaves.class);

		private static final String SLAVES = "slaves";

		private String monitorName;

		public SentinelSlaves(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
							  String monitorName) {

			super(clientPool, scheduled);
			this.monitorName = monitorName;
		}


		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, SLAVES, monitorName).format();
		}

		@Override
		protected List<HostPort> format(Object payload) {
			if(!(payload instanceof Object[])){
				throw new IllegalStateException("expected Object[], but:" + payload + "," + payload.getClass());
			}
			return doFormat((Object[])payload);
		}

		private List<HostPort> doFormat(Object[] payload) {
			List<HostPort> slaves = Lists.newArrayListWithExpectedSize(payload.length);
			for(Object slaveObj : payload) {
				if(!(slaveObj instanceof Object[])) {
					logger.error("[doFormat] unexpected response: {}", slaveObj);
					continue;
				}
				Object[] slaveInfoElements = (Object[]) slaveObj;
				slaves.add(new HostPort(payloadToString(slaveInfoElements[3]), payloadToInteger(slaveInfoElements[5])));
			}
			return slaves;
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}

	public static class SentinelReset extends AbstractSentinelCommand<Long>{

		private static final Logger logger = LoggerFactory.getLogger(SentinelRemove.class);

		public static String RESET = "reset";

		private String masterName;

		public SentinelReset(SimpleObjectPool<NettyClient> clientPool, String masterName, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.masterName = masterName;
		}

		@Override
		protected Long format(Object payload) {
			return payloadToLong(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, RESET, masterName).format();
		}

		@Override
		public String toString() {
			return String.format("%s %s %s", SENTINEL, RESET, masterName);
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}

	public static class SentinelMonitor extends AbstractSentinelCommand<String>{

		private static final Logger logger = LoggerFactory.getLogger(SentinelMonitor.class);

		public static String MONITOR = "monitor";

		private String monitorName;

		private HostPort master;

		private int quorum;

		public SentinelMonitor(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
							 String monitorName, HostPort master, int quorum) {
			super(clientPool, scheduled);
			this.monitorName = monitorName;
			this.master = master;
			this.quorum = quorum;
		}

		public SentinelMonitor(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
							   String monitorName, HostPort master, int quorum, int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
			this.monitorName = monitorName;
			this.master = master;
			this.quorum = quorum;
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, MONITOR, monitorName, master.getHost(),
					Integer.toString(master.getPort()), Integer.toString(quorum)).format();
		}

		@Override
		public String toString() {
			return String.format("%s %s %s", SENTINEL, MONITOR, monitorName);
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}

	public static class SentinelSet extends AbstractSentinelCommand<String>{

		private static final Logger logger = LoggerFactory.getLogger(SentinelMonitor.class);

		public static String SET = "set";

		private String monitorName;

		private String[] masterConfig;

		public SentinelSet(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
							   String monitorName, String[] masterConfig) {
			super(clientPool, scheduled);
			this.monitorName = monitorName;
			this.masterConfig = masterConfig;
		}

		public SentinelSet(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
							   String monitorName, String[] masterConfig, int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
			this.monitorName = monitorName;
			this.masterConfig = masterConfig;
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			String[] commandArray = new String[]{SENTINEL, SET, monitorName};
			return new RequestStringParser(ArrayUtils.addAll(commandArray, masterConfig)).format();
		}

		@Override
		public String toString() {
			return String.format("%s %s %s %s", SENTINEL, SET, monitorName, StringUtils.join(masterConfig, " "));
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}

	public static class SentinelConfigSet extends AbstractSentinelCommand<String>{

		private static final Logger logger = LoggerFactory.getLogger(SentinelMonitor.class);

		public static String CONFIG = "CONFIG";

		public static String SET = "SET";

		private String configName;

		private String configValue;

		public SentinelConfigSet(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
				 String configName, String configValue) {
			super(clientPool, scheduled);
			this.configName = configName;
			this.configValue = configValue;
		}

		public SentinelConfigSet(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
				 String configName, String configValue, int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
			this.configName = configName;
			this.configValue = configValue;
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(SENTINEL, CONFIG, SET, configName, configValue).format();
		}

		@Override
		public String toString() {
			return String.format("%s %s %s %s %s", SENTINEL, CONFIG, SET, configName, configValue);
		}

		@Override
		protected Logger getLogger() {
			return logger;
		}
	}
}
