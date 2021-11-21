package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *
 * Nov 11, 2016
 */
public class MasterRole extends AbstractRole{

	private long offset;

	private List<Slave> slaves;

	public MasterRole(Object []payload) {
		expectedLen(payload, 3);
		this.serverRole = SERVER_ROLE.of(payload[0].toString());
		this.offset = Long.parseLong(payload[1].toString());

		List<Slave> list = new ArrayList<>();
		if (payload[2] instanceof Object[]) {
			for (Object slavePayload: (Object[])payload[2]) {
				if (slavePayload instanceof Object[] && ((Object[]) slavePayload).length == 3)
					list.add(new Slave((Object[])slavePayload));
			}
		}
		this.slaves = Collections.unmodifiableList(list);
	}

	public List<Slave> getSlaves() {
		return this.slaves;
	}

	public List<HostPort> getSlaveHostPorts() {
		return this.slaves.stream().map(Slave::getHostPort).collect(Collectors.toList());
	}

	public MasterRole(){
		serverRole = SERVER_ROLE.MASTER;
	}

	@Override
	public ByteBuf format() {
		Object[] tmp = new Object[] { serverRole.toString(), offset, new Object[0]};
		return new ArrayParser(tmp).format();
	}

	@Override
	public String toString() {
		return String.format("role:%s, slaves:%s", serverRole, slaves);
	}

	public static class Slave {

		private String host;

		private int port;

		private long lastAckOffset;

		public Slave(Object[] payload) {
			this.host = payload[0].toString();
			this.port = Integer.parseInt(payload[1].toString());
			this.lastAckOffset = Long.parseLong(payload[2].toString());
		}

		public HostPort getHostPort() {
			return new HostPort(host, port);
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public long getLastAckOffset() {
			return lastAckOffset;
		}

		@Override
		public String toString() {
			return String.format("%s:%d", host, port);
		}

	}

}
