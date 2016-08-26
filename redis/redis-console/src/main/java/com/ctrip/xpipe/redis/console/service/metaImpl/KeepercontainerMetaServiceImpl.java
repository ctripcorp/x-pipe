package com.ctrip.xpipe.redis.console.service.metaImpl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.service.meta.KeepercontainerMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import org.springframework.stereotype.Service;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("keepercontainerMetaService")
public class KeepercontainerMetaServiceImpl implements KeepercontainerMetaService {

	@Override
	public KeeperContainerMeta encodeKeepercontainerMeta(KeepercontainerTbl keepercontainer, DcMeta dcMeta) {
		KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
		
		keeperContainerMeta.setId(keepercontainer.getKeepercontainerId());
		keeperContainerMeta.setIp(keepercontainer.getKeepercontainerIp());
		keeperContainerMeta.setPort(keepercontainer.getKeepercontainerPort());
		keeperContainerMeta.setParent(dcMeta);

		return keeperContainerMeta;
	}

}
