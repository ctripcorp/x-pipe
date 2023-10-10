package com.ctrip.xpipe.redis.console.cache;

import com.ctrip.xpipe.redis.console.model.AzGroupModel;

import java.util.List;

public interface AzGroupCache {

    List<AzGroupModel> getAllAzGroup();

    AzGroupModel getAzGroupById(Long id);

    AzGroupModel getAzGroupByAzs(List<String> azs);

}
