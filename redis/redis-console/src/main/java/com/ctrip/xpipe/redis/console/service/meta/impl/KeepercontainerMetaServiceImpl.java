package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.KeepercontainerMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service
public class KeepercontainerMetaServiceImpl extends AbstractMetaService implements KeepercontainerMetaService {

	@Autowired
	private LogicalBuService logicalBuService;

	@Override
	public KeeperContainerMeta encodeKeepercontainerMeta(KeepercontainerTbl keepercontainer, DcMeta dcMeta) {
		KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
		
		if(null != keepercontainer) {
			keeperContainerMeta.setId(keepercontainer.getKeepercontainerId());
			keeperContainerMeta.setIp(keepercontainer.getKeepercontainerIp());
			keeperContainerMeta.setPort(keepercontainer.getKeepercontainerPort());
			keeperContainerMeta.setDiskType(keepercontainer.getKeepercontainerDiskType());
			keeperContainerMeta.setTag(keepercontainer.getTag());
			keeperContainerMeta.setParent(dcMeta);
			if(keepercontainer.getAzId() != 0)
				keeperContainerMeta.setAzId(keepercontainer.getAzId());
			if (keepercontainer.getLogicalBuId() > 0) {
				keeperContainerMeta.setLogicalBuId(keepercontainer.getLogicalBuId());
				LogicalBuModel logicalBu = logicalBuService.findById(keepercontainer.getLogicalBuId());
				keeperContainerMeta.setTfsFsId(logicalBu.getTfsFsId());
			}
		}
		
		return keeperContainerMeta;
	}

}
