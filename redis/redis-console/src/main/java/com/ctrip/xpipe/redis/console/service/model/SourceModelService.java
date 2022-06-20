package com.ctrip.xpipe.redis.console.service.model;

import com.ctrip.xpipe.redis.console.model.SourceModel;

import java.util.List;

public interface SourceModelService {

    List<SourceModel> getAllSourceModels(String dcName, String clusterName);

}
