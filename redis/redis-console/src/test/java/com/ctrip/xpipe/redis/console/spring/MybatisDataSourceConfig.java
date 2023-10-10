//package com.ctrip.xpipe.redis.console.spring;
//
//import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
//import com.ctrip.datasource.configure.DalDataSourceFactory;
//import com.ctrip.xpipe.spring.AbstractProfile;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Profile;
//
//import javax.sql.DataSource;
//
//@Configuration
//@Profile(AbstractProfile.PROFILE_NAME_TEST)
//public class MybatisDataSourceConfig {
//
//    @Bean
//    public MybatisSqlSessionFactoryBean mybatisSqlSessionFactoryBean() throws Exception {
//        MybatisSqlSessionFactoryBean sqlSessionFactoryBean = new MybatisSqlSessionFactoryBean();
//
//        sqlSessionFactoryBean.setDataSource(dataSource());
//        return sqlSessionFactoryBean;
//    }
//
//    @Bean
//    public DataSource dataSource() throws Exception {
//        DalDataSourceFactory factory = new DalDataSourceFactory();
//        return factory.getOrCreateDataSource("fxxpipedb_dalcluster");
//    }
//
//}
