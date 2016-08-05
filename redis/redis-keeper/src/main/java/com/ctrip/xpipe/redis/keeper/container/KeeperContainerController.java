package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@RestController
@RequestMapping("/keepers")
public class KeeperContainerController {
    @Autowired
    private KeeperContainerService keeperContainerService;

    @RequestMapping(method = RequestMethod.POST)
    public void add(@RequestBody KeeperTransMeta keeperTransMeta) {
        keeperContainerService.add(keeperTransMeta);
    }

}
