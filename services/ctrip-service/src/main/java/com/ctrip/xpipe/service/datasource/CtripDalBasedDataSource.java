package com.ctrip.xpipe.service.datasource;

import com.ctrip.platform.dal.dao.configure.SwitchableDataSourceStatus;
import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSource;
import com.ctrip.platform.dal.dao.datasource.IForceSwitchableDataSource;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.service.fireman.ForceSwitchableDataSourceHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;

import java.sql.Connection;
import java.sql.SQLException;

public class CtripDalBasedDataSource implements DataSource {

    private static final Logger logger = LoggerFactory.getLogger(CtripDalBasedDataSource.class);

    private ForceSwitchableDataSource dataSource;

    private DataSourceDescriptor descriptor;

    private void init() throws SQLException {
        dataSource = new ForceSwitchableDataSource(new XPipeDataSourceConfigureProvider(descriptor));
        ForceSwitchableDataSourceHolder.getInstance().setDataSource(dataSource);
        dataSource.addListener(new IForceSwitchableDataSource.SwitchListener() {
            @Override
            public void onForceSwitchSuccess(SwitchableDataSourceStatus currentStatus) {
                logger.info("[onForceSwitchSuccess] current status, {}", currentStatus);
                EventMonitor.DEFAULT.logEvent("DAL.SWITCH", "SUCCESS");
            }

            @Override
            public void onForceSwitchFail(SwitchableDataSourceStatus currentStatus, Throwable cause) {
                logger.error("[onForceSwitchFail] current status, {}", currentStatus, cause);
                EventMonitor.DEFAULT.logEvent("DAL.SWITCH", "FAILURE");
            }

            @Override
            public void onRestoreSuccess(SwitchableDataSourceStatus currentStatus) {
                logger.info("[onRestoreSuccess] current status, {}", currentStatus);
                EventMonitor.DEFAULT.logEvent("DAL.RESTORE", "SUCCESS");
            }

            @Override
            public void onRestoreFail(SwitchableDataSourceStatus currentStatus, Throwable cause) {
                logger.error("[onRestoreFail] current status, {}", currentStatus, cause);
                EventMonitor.DEFAULT.logEvent("DAL.RESTORE", "FAILURE");
            }
        });

    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public DataSourceDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public void initialize(DataSourceDescriptor descriptor) {
        this.descriptor = descriptor;
        try {
            init();
        } catch (SQLException e) {
            logger.error("[initialize]", e);
        }
    }
}
