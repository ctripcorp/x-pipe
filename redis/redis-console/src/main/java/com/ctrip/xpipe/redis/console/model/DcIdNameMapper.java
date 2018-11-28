package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.service.DcService;

import java.util.Map;

public interface DcIdNameMapper {

    String getName(long id);

    long getId(String name);

    public class DefaultMapper implements DcIdNameMapper {

        private Map<Long, String> dcIdNameMap;

        public DefaultMapper(DcService dcService) {
            this.dcIdNameMap = dcService.dcNameMap();
        }

        @Override
        public String getName(long id) {
            String name = dcIdNameMap.get(id);
            if(name == null) {
                throw new XpipeRuntimeException(String.format("dc id: %d not found", id));
            }
            return name;
        }

        @Override
        public long getId(String name) {
            for(Map.Entry<Long, String> entry : dcIdNameMap.entrySet()) {
                if(entry.getValue().equalsIgnoreCase(name)) {
                    return entry.getKey();
                }
            }
            throw new XpipeRuntimeException(String.format("dc name: %d not found", name));
        }
    }

    public class OneTimeMapper implements DcIdNameMapper {

        private DcService dcService;

        public OneTimeMapper(DcService dcService) {
            this.dcService = dcService;
        }

        @Override
        public String getName(long id) {
            return dcService.getDcName(id);
        }

        @Override
        public long getId(String name) {
            return dcService.find(name).getId();
        }
    }
}
