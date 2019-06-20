package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.spi.FiremanDependency;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSource;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class XPipeFiremanDependency implements FiremanDependency {

    private static Logger logger = LoggerFactory.getLogger(XPipeFiremanDependency.class);

    @Override
    public List<String> getAppIds() {
        return Lists.newArrayList("100004374");
    }

    @Override
    public String getDatabaseDomainName() {
        return Environment.getInstance().getDatabaseDomainName();
    }

    @Override
    public ForceSwitchableDataSource getDataSource() {
        return ForceSwitchableDataSourceHolder.getInstance().getDataSource();
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

    private enum Environment {
        PRO {
            @Override
            String getDatabaseDomainName() {
                return "fxxpipe.mysql.db.ctripcorp.com";
            }
        },
        UAT {
            @Override
            String getDatabaseDomainName() {
                return "fxxpipe.mysql.db.uat.qa.nt.ctripcorp.com";
            }
        },
        FAT {
            @Override
            String getDatabaseDomainName() {
                return "fxxpipe.mysql.db.fat.qa.nt.ctripcorp.com";
            }
        };

        public static Environment getInstance() {
            try {
                return valueOf(Foundation.server().getEnvFamily().getName());
            } catch (Exception e) {
                logger.error("[Environment][getInstance]", e);
            }
            return PRO;
        }

        abstract String getDatabaseDomainName();
    }
}
