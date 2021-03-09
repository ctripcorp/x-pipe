package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.checker;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.VariableChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

abstract class AbstractVariableChecker implements VariableChecker {
    abstract boolean checkValue(Object value);
    abstract String getName();

    private AlertManager alertManager;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SHOW_VARIABLES_TEMPLATE = "show global variables like \"%s\"";
    private static final String COLUMN_VALUE = "Value";

    public AbstractVariableChecker(AlertManager alertManager) {
        this.alertManager = alertManager;
    }

    public void check(DataSource dataSource) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            logger.debug("[check] start check variables {} for {} ", getName(), dataSource.getDescriptor());

            connection = dataSource.getConnection();
            statement = connection.prepareStatement(String.format(SHOW_VARIABLES_TEMPLATE, getName()),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();

            if (!resultSet.first()) {
                logger.info("[check] instance {} get variables {} empty", dataSource, getName());
            } else if (!checkValue(resultSet.getObject(COLUMN_VALUE))) {
                logger.debug("[check] unexpected variables {} value {}", getName(), resultSet.getObject(COLUMN_VALUE));
                alertMessage(dataSource, resultSet.getObject(COLUMN_VALUE));
            }
        } catch (Exception e) {
            // skip check when query fail
            logger.info("[check] check for {} fail", getName(), e);
        } finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    logger.info("[check] release resource ResultSet fail", e);
                }
            }
            if (null != statement) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.info("[check] release resource PreparedStatement fail", e);
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.info("[check] release resource Connection fail", e);
                }
            }
        }
    }

    protected void alertMessage(DataSource dataSource, Object value) {
        alertManager.alert("", "", null, ALERT_TYPE.DB_VARIABLES_INVALIDATE,
                String.format("url:%s, variable:%s, value:%s",
                        dataSource.getDescriptor().getProperty("url", null), getName(), value));
    }
}
