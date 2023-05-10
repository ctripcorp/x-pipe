package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.web.client.RestOperations;

public class DefaultHttpService extends AbstractService {

    public DefaultHttpService() {
    }

    public RestOperations getRestTemplate() {
        return restTemplate;
    }
}
