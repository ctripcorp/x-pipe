package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.AzGroupModel;

import java.util.List;

public interface AzGroupService {

    void create(String name, List<String> azs);

    void deleteByName(String name);

    List<AzGroupModel> getAll();

}
