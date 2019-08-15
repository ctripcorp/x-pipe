package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.spi.FiremanDependency;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSource;
import com.ctrip.xpipe.service.config.ApolloConfig;
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
    public int mhaSwitchMaxExecuteTimeoutS() {
        return Integer.parseInt(ApolloConfig.DEFAULT.get("xpipe.mha.switch.timeout", "100"));
    }

    @Override
    public boolean openAvailableCheckTask() {
        return true;
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

    public enum Environment {
        PRO {

            final String DB_DOMAIN = "fxxpipe.mysql.db.ctripcorp.com";

            @Override
            String getDatabaseDomainName() {
                return DB_DOMAIN;
            }

        },
        UAT {

            final String DB_DOMAIN = "fxxpipe.mysql.db.uat.qa.nt.ctripcorp.com";

            @Override
            String getDatabaseDomainName() {
                return DB_DOMAIN;
            }
        },
        FAT {

            final String DB_DOMAIN = "fxxpipe.mysql.db.fat.qa.nt.ctripcorp.com";

            @Override
            String getDatabaseDomainName() {
                return DB_DOMAIN;
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
