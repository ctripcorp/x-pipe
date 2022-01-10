package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.AzMetaService;
import com.ctrip.xpipe.redis.core.entity.AzMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.stereotype.Service;

@Service
public class AzMetaServiceImpl extends AbstractMetaService implements AzMetaService {
    @Override
    public AzMeta encodeAzMeta(AzTbl azTbl, DcMeta dcMeta) {
        AzMeta azMeta = new AzMeta();

        if(null != azTbl) {
            azMeta.setId(azTbl.getAzName());
            azMeta.setActive(azTbl.isActive());
            azMeta.setParent(dcMeta);
        }

        return azMeta;
    }
}
