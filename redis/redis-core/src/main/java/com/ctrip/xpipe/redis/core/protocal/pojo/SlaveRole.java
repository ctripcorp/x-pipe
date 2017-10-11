package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.utils.ObjectUtils;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 *         Sep 16, 2016
 */
public class SlaveRole extends AbstractRole {

	private String masterHost;
	private int masterPort;
	private MASTER_STATE masterState;
	private long masterOffset;

	public SlaveRole(Object[] payload) {
		expectedLen(payload, 5);
		this.serverRole = SERVER_ROLE.of(payload[0].toString());
		this.masterHost = payload[1].toString();
		this.masterPort = Integer.parseInt(payload[2].toString());
		this.masterState = MASTER_STATE.fromDesc(payload[3].toString());
		this.masterOffset = Long.parseLong(payload[4].toString());
	}

	public SlaveRole(SERVER_ROLE serverRole, String masterHost, int masterPort, MASTER_STATE masterState,
			long masterOffset) {
		this.serverRole = serverRole;
		this.masterHost = masterHost;
		this.masterPort = masterPort;
		this.masterState = masterState;
		this.masterOffset = masterOffset;
	}

	public String getMasterHost() {
		return masterHost;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public MASTER_STATE getMasterState() {
		return masterState;
	}

	public long getMasterOffset() {
		return masterOffset;
	}

	@Override
	public ByteBuf format() {
		Object[] tmp = new Object[] { serverRole.toString(), masterHost, masterPort, masterState.getDesc(),
				masterOffset };
		return new ArrayParser(tmp).format();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof SlaveRole)) {
			return false;
		}

		SlaveRole other = (SlaveRole) obj;
		if (!(ObjectUtils.equals(serverRole, other.serverRole))) {
			return false;
		}
		if (!(ObjectUtils.equals(masterHost, other.masterHost))) {
			return false;
		}
		if (!(ObjectUtils.equals(masterPort, other.masterPort))) {
			return false;
		}
		if (!(ObjectUtils.equals(masterState, other.masterState))) {
			return false;
		}
		if (!(ObjectUtils.equals(masterPort, other.masterPort))) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return ObjectUtils.hashCode(serverRole, masterHost, masterPort, masterState, masterOffset);
	}

	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}
}
