package com.ctrip.xpipe.redis.console.ds;

import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.console.model.ConfigTblDao;
import com.ctrip.xpipe.redis.console.model.ConfigTblEntity;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.apache.ibatis.datasource.DataSourceException;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.sql.Connection;
import java.sql.SQLException;

@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@MapperScan("com.ctrip.xpipe.redis.console.mapper")
@EnableTransactionManagement
public class MybatisDataSourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(MybatisDataSourceConfig.class);
    private XPipeDataSource dataSource;

    private CommonConfigBean commonConfigBean = new CommonConfigBean();


    @Bean
    public MybatisSqlSessionFactoryBean mybatisSqlSessionFactoryBean(PlexusContainer container) throws Exception {
        MybatisSqlSessionFactoryBean sqlSessionFactoryBean = new MybatisSqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(dataSource(container));
        return sqlSessionFactoryBean;
    }

    @Bean
    public javax.sql.DataSource dataSource(PlexusContainer container) throws Exception {

        if(commonConfigBean.disableDb()) {
            // if disableDb is true, set a fake datasource, only for autowired.
            return makeFakeDataSource();
        }
        // 强制查询使Xpipe DataSource初始化
        ConfigTblDao configTblDao = container.lookup(ConfigTblDao.class);
        configTblDao.findByPK(1L, ConfigTblEntity.READSET_FULL);

        XPipeDataSource dataSource = tryGetXpipeDataSource(container);
        if (dataSource == null) {
            logger.info("[mybatisSqlSessionFactoryBean] no xpipe datasource found");
            throw new DataSourceException("no xpipe datasource found");
        }
        return dataSource.getBaseDataSource();
    }


    private XPipeDataSource tryGetXpipeDataSource(PlexusContainer container) {
        if (this.dataSource != null) {
            return this.dataSource;
        }
        synchronized (this) {
            if (this.dataSource != null) {
                return this.dataSource;
            }
            try {
                DataSourceManager dataSourceManager = container.lookup(DataSourceManager.class);
                if (dataSourceManager.getDataSourceNames().isEmpty()) {
                    logger.info("[tryGetDataSource] no datasource found");
                } else {
                    String datasourceName = dataSourceManager.getDataSourceNames().get(0);
                    DataSource dataSource = dataSourceManager.getDataSource(datasourceName);
                    if (dataSource instanceof XPipeDataSource) {
                        this.dataSource = (XPipeDataSource) dataSource;
                    }
                }
            } catch (ComponentLookupException e) {
                logger.info("[tryGetDataSource] xpipe datasource miss");
            } catch (Throwable th) {
                logger.info("[tryGetDataSource] fail", th);
            }

            return this.dataSource;
        }
    }

    private javax.sql.DataSource makeFakeDataSource() {
        return new AbstractDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("This is a fake datasource");
            }
            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                throw new SQLException("This is a fake datasource");
            }
        };
    }
}
