package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import org.junit.Assert;

import java.util.concurrent.ExecutionException;

public class AbstractKeeperIntegratedMultiDcXsync extends AbstractKeeperIntegratedMultiDc {

}
