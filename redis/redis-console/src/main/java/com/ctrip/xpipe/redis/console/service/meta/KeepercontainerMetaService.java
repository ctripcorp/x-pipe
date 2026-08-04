package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;

import java.util.Map;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface KeepercontainerMetaService {
	/**
	 * @param logicalBuTfsFsIdById preloaded logicalBuId → tfsFsId (D24); must not query per KC
	 */
	KeeperContainerMeta encodeKeepercontainerMeta(KeepercontainerTbl keepercontainer, DcMeta dcMeta,
												  Map<Long, String> logicalBuTfsFsIdById);
}
