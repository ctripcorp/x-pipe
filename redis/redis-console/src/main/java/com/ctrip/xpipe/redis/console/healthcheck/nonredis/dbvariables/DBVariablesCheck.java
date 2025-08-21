package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.util.*;

@Component
public class DBVariablesCheck extends AbstractCrossDcIntervalAction {

    @Autowired
    private List<VariableChecker> variableCheckers;

    private DataSourceManager dataSourceManager;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.DB_VARIABLES_INVALIDATE);

    private Map<String, DatabaseVariablesCheck> databaseVariablesCheckMap = new HashMap<>();

    private Set<String> interestedDataSources = new HashSet<>();

    @PostConstruct
    @Override
    public void postConstruct() {
        try {
            dataSourceManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
            super.postConstruct(); // not start check if load DataSourceManager fail.
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct DataSourceManager.", e);
        }
    }

    protected void doAction() {
        refreshInterestedDataSources();

        interestedDataSources.forEach(dataSourceName -> {
            DataSource dataSource = dataSourceManager.getDataSource(dataSourceName);
            if (null == dataSource) return;

            DatabaseVariablesCheck databaseCheck = MapUtils.getOrCreate(databaseVariablesCheckMap, dataSourceName, () -> {
                    DatabaseVariablesCheck databaseVariablesCheck = new DatabaseVariablesCheck();
                    variableCheckers.forEach(databaseVariablesCheck::addChecker);
                    return databaseVariablesCheck;
            });
            databaseCheck.check(dataSource);
        });
    }

    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

    protected void refreshInterestedDataSources() {
        Set<String> checkDataSources = consoleConfig.getVariablesCheckDataSources();
        List<String> toRemove = new ArrayList<>();

        interestedDataSources.forEach(datasourceName -> {
            if (!checkDataSources.contains(datasourceName)) toRemove.add(datasourceName);
        });
        interestedDataSources.addAll(checkDataSources);

        toRemove.forEach(datasourceName -> {
            interestedDataSources.remove(datasourceName);
            DatabaseVariablesCheck check = databaseVariablesCheckMap.remove(datasourceName);
            if (null != check) check.release();
        });
    }

    @VisibleForTesting
    protected void setVariableCheckers(List<VariableChecker> checkers) {
        this.variableCheckers = checkers;
    }

    @VisibleForTesting
    protected void setDataSourceManager(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    @VisibleForTesting
    protected void setConsoleConfig(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
    }

    @VisibleForTesting
    protected Set<String> getInterestedDataSources() {
        return this.interestedDataSources;
    }

}
