package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.container.database.DbNode;
import com.ctrip.framework.fireman.container.manager.MhaManagerNode;
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
        return Integer.parseInt(ApolloConfig.DEFAULT.get("xpipe.mha.switch.timeout", "30"));
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

    public enum Environment {
        PRO {

            final String DB_DOMAIN = "fxxpipe.mysql.db.ctripcorp.com";
            final String MASTER_IP = ApolloConfig.DEFAULT.get("xpipe.mysql.master.ip", "10.9.72.48");
            final String MASTER_LOCATE_SHORT = "SHAFQ";
            final int MASTER_PORT = 55944;

            final String MHA_MANAGER_IP = ApolloConfig.DEFAULT.get("xpipe.mha.manager.ip", "10.0.0.0");
            final String MHA_MANAGER_LOCATE = "SHAFQ";

            @Override
            String getDatabaseDomainName() {
                return DB_DOMAIN;
            }

            @Override
            DbNode createMasterNode() {
                return newDbNode(MASTER_PORT, MASTER_IP, MASTER_LOCATE_SHORT);
            }

            @Override
            MhaManagerNode createMhaManagerNode() {
                return newMhaManagerNode(MHA_MANAGER_IP, MHA_MANAGER_LOCATE);
            }
        },
        UAT {

            final String DB_DOMAIN = "fxxpipe.mysql.db.uat.qa.nt.ctripcorp.com";
            final String MASTER_IP = ApolloConfig.DEFAULT.get("xpipe.mysql.master.ip", "10.5.165.240");
            final String MASTER_LOCATE_SHORT = "NTGXH";
            final int MASTER_PORT = 55777;

            final String MHA_MANAGER_IP = "10.5.109.129";
            final String MHA_MANAGER_LOCATE = "NTGXH";

            @Override
            String getDatabaseDomainName() {
                return DB_DOMAIN;
            }

            @Override
            DbNode createMasterNode() {
                return newDbNode(MASTER_PORT, MASTER_IP, MASTER_LOCATE_SHORT);
            }



            @Override
            MhaManagerNode createMhaManagerNode() {
                return newMhaManagerNode(MHA_MANAGER_IP, MHA_MANAGER_LOCATE);
            }
        },
        FAT {

            final String DB_DOMAIN = "fxxpipe.mysql.db.fat.qa.nt.ctripcorp.com";
            final String MASTER_IP = "10.5.21.119";
            final String MASTER_LOCATE_SHORT = "NTGXH";
            final int MASTER_PORT = 55111;

            final String MHA_MANAGER_IP = "10.0.0.0";//"10.2.61.104";
            final String MHA_MANAGER_LOCATE = "NTGXH";

            @Override
            String getDatabaseDomainName() {
                return DB_DOMAIN;
            }

            @Override
            DbNode createMasterNode() {
                return newDbNode(MASTER_PORT, MASTER_IP, MASTER_LOCATE_SHORT);
            }

            @Override
            MhaManagerNode createMhaManagerNode() {
                return newMhaManagerNode(MHA_MANAGER_IP, MHA_MANAGER_LOCATE);
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

        abstract DbNode createMasterNode();

        abstract MhaManagerNode createMhaManagerNode();

        protected DbNode masterNode;

        protected MhaManagerNode mhaManagerNode;

        protected MhaManagerNode getMhaManagerNode() {
            if (mhaManagerNode == null) {
                synchronized (this) {
                    if(mhaManagerNode == null) {
                        mhaManagerNode = createMhaManagerNode();
                    }
                }
            }
            return mhaManagerNode;
        }

        protected DbNode getMasterNode() {
            if (masterNode == null) {
                synchronized (this) {
                    if(masterNode == null) {
                        masterNode = createMasterNode();
                    }
                }
            }
            return masterNode;
        }

        private static DbNode newDbNode(int master_port, String master_ip, String master_locate_short) {
            DbNode dbNode = new DbNode();
            dbNode.setDns_port(master_port);
            dbNode.setService_ip(master_ip);
            dbNode.setMachine_located_short(master_locate_short);
            return dbNode;
        }

        private static MhaManagerNode newMhaManagerNode(String ip, String locateShort) {
            MhaManagerNode mhanode = new MhaManagerNode();
            mhanode.setIp(ip);
            mhanode.setMachine_located_short(locateShort);
            return mhanode;
        }
    }
}
