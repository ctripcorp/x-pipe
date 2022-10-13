package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.AppliercontainerTbl;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.AppliercontainerMetaService;
import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.stereotype.Service;

/**
 * @author ayq
 * <p>
 * 2022/6/15 17:25
 */
@Service
public class AppliercontainerMetaServiceImpl extends AbstractMetaService implements AppliercontainerMetaService {

    @Override
    public ApplierContainerMeta encodeAppliercontainerMeta(AppliercontainerTbl appliercontainer, DcMeta dcMeta) {
        ApplierContainerMeta applierContainerMeta = new ApplierContainerMeta();

        if(null != appliercontainer) {
            applierContainerMeta.setId(appliercontainer.getAppliercontainerId());
            applierContainerMeta.setIp(appliercontainer.getAppliercontainerIp());
            applierContainerMeta.setPort(appliercontainer.getAppliercontainerPort());
            applierContainerMeta.setParent(dcMeta);
            if(appliercontainer.getAppliercontainerAz() != 0)
                applierContainerMeta.setAzId(appliercontainer.getAppliercontainerAz());
        }

        return applierContainerMeta;
    }
}
