package com.ctrip.xpipe.redis.console.ds;

import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.ctrip.datasource.configure.DalDataSourceFactory;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.codehaus.plexus.PlexusContainer;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.sql.Connection;
import java.sql.SQLException;

@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@MapperScan("com.ctrip.xpipe.redis.console.mapper")
@EnableTransactionManagement
public class MybatisDataSourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(MybatisDataSourceConfig.class);

    private CommonConfigBean commonConfigBean = new CommonConfigBean();

    private static final String DB = "fxxpipedb_dalcluster";

    private DalDataSourceFactory factory = new DalDataSourceFactory();


    @Bean
    public MybatisSqlSessionFactoryBean mybatisSqlSessionFactoryBean(javax.sql.DataSource dataSourceBean) throws Exception {
        MybatisSqlSessionFactoryBean sqlSessionFactoryBean = new MybatisSqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(dataSourceBean);
        return sqlSessionFactoryBean;
    }

    @Bean
    public javax.sql.DataSource dataSource(PlexusContainer container) throws Exception {

        if(commonConfigBean.disableDb()) {
            // if disableDb is true, set a fake datasource, only for autowired.
            return makeFakeDataSource();
        }

        return factory.getOrCreateDataSource(DB);
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
