package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public interface MetaCache {

    List<DcMeta> getDcMetas();

    XpipeMeta getXpipeMeta();

    boolean inBackupDc(HostPort hostPort);

    HostPort findMasterInSameShard(HostPort hostPort);

}
