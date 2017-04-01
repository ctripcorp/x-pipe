package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
@Component
@Lazy
public class DefaultMetaCache implements  MetaCache{

    private int loadIntervalSeconds = 30;

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private DcService dcService;

    private List<DcMeta> dcMetas;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    public DefaultMetaCache(){
        System.out.println("===============");
    }

    @PostConstruct
    public void postConstruct(){

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                loadCache();
            }
        }, 5, loadIntervalSeconds, TimeUnit.SECONDS);
    }

    private void loadCache() {

        List<DcTbl> dcs = dcService.findAllDcNames();

        List<DcMeta> dcMetas = new LinkedList<>();
        for (DcTbl dc : dcs) {
            dcMetas.add(dcMetaService.getDcMeta(dc.getDcName()));
        }

            this.dcMetas = dcMetas;
    }

    @Override
    public List<DcMeta> getDcMetas() {
        return dcMetas;
    }
}
