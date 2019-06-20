package com.ctrip.xpipe.service.config;

import com.ctrip.framework.apollo.ds.ApolloDataSourceProvider;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.codehaus.plexus.logging.Logger;
import org.junit.Test;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 29, 2017
 */
public class ApolloDataSourceProviderTest extends AbstractServiceTest{

    @Test
    public void test(){

        ApolloDataSourceProvider apolloDataSourceProvider = new ApolloDataSourceProvider();
        apolloDataSourceProvider.enableLogging(new Logger() {
            @Override
            public void debug(String message) {

            }

            @Override
            public void debug(String message, Throwable throwable) {

            }

            @Override
            public boolean isDebugEnabled() {
                return false;
            }

            @Override
            public void info(String message) {

            }

            @Override
            public void info(String message, Throwable throwable) {

            }

            @Override
            public boolean isInfoEnabled() {
                return false;
            }

            @Override
            public void warn(String message) {

            }

            @Override
            public void warn(String message, Throwable throwable) {

            }

            @Override
            public boolean isWarnEnabled() {
                return false;
            }

            @Override
            public void error(String message) {

            }

            @Override
            public void error(String message, Throwable throwable) {

            }

            @Override
            public boolean isErrorEnabled() {
                return false;
            }

            @Override
            public void fatalError(String message) {

            }

            @Override
            public void fatalError(String message, Throwable throwable) {

            }

            @Override
            public boolean isFatalErrorEnabled() {
                return false;
            }

            @Override
            public Logger getChildLogger(String name) {
                return null;
            }

            @Override
            public int getThreshold() {
                return 0;
            }

            @Override
            public void setThreshold(int threshold) {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        DataSourcesDef dataSourcesDef = apolloDataSourceProvider.defineDatasources();
        logger.info("{}", dataSourcesDef);


    }
}
