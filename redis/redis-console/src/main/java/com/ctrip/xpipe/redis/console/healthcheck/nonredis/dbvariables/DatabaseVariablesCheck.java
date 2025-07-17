package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.endpoint.HostPort;
import org.codehaus.plexus.PlexusContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;
import org.unidal.lookup.ContainerLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseVariablesCheck implements Releasable, VariableChecker {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseVariablesCheck.class);

    private static final Pattern dbUrlPattern = Pattern.compile("^jdbc:([a-zA-Z]*:)+//[^/;?&]+");

    private String dbUrlPrefix = null;

    private String checkId = null;

    List<VariableChecker> checkers = new ArrayList<>();

    List<DataSource> instanceDataSources = new ArrayList<>();

    private PlexusContainer plexusContainer;

    public void setPlexusContainer(PlexusContainer plexusContainer) {
        this.plexusContainer = plexusContainer;
    }

    public void check(DataSource dataSource) {
        if (null == this.checkId) init(dataSource);
        String datasourceId = dataSource.getDescriptor().getId();
        if (!this.checkId.equals(datasourceId)) {
            logger.warn("[check] expected check datasource id {} but get {}", this.checkId, datasourceId);
            return;
        }

        this.instanceDataSources.forEach(instanceDataSource -> this.checkers.forEach(checker -> checker.check(instanceDataSource)));
    }

    private void init(DataSource dataSource) {
        DataSourceDescriptor descriptor = dataSource.getDescriptor();
        this.checkId = descriptor.getId();

        List<HostPort> instances = parseInstanceInUrl(descriptor.getProperty("url", null));
        instances.forEach(hostPort -> {
            DataSourceDescriptor instanceDescriptor = makeDescriptor(hostPort, descriptor);
            addDataSource(makeDataSource(instanceDescriptor));
        });
    }

    private List<HostPort> parseInstanceInUrl(String url) {
        if (null == url) return Collections.emptyList();

        List<HostPort> instances = new ArrayList<>();
        Matcher match = dbUrlPattern.matcher(url);
        if (!match.find()) return instances;

        String dbUrl = match.group();
        String[] dbUrlParts = dbUrl.split("/");

        dbUrlPrefix = dbUrlParts[0];
        String hostsStr = dbUrlParts[2];
        Arrays.stream(hostsStr.split("\\s*,\\s*")).forEach(hostStr -> {
          if (hostStr.contains(":")) instances.add(HostPort.fromString(hostStr));
          else instances.add(new HostPort(hostStr, 3306));
        });

        return instances;
    }

    private DataSourceDescriptor makeDescriptor(HostPort hostPort, DataSourceDescriptor origin) {
        JdbcDataSourceDescriptor descriptor = new JdbcDataSourceDescriptor();
        descriptor.setId("single:" + hostPort);
        descriptor.setType(origin.getType());
        descriptor.setProperty("url", String.format("%s//%s", dbUrlPrefix, hostPort));
        descriptor.setProperty("driver", origin.getProperty("driver", null));
        descriptor.setProperty("user", origin.getProperty("user", null));
        descriptor.setProperty("password", origin.getProperty("password", null));
        return descriptor;
    }

    private DataSource makeDataSource(DataSourceDescriptor descriptor) {
        DataSource dataSource = null;
        try {
            dataSource = plexusContainer.lookup(DataSource.class, descriptor.getType());
            dataSource.initialize(descriptor);
        } catch (Exception e) {
            logger.error("getDatasource type {} url {} fail",
                    descriptor.getType(), descriptor.getProperty("url", null), e);
        }

        return dataSource;
    }

    public void addChecker(VariableChecker checker) {
        if (null != checker) this.checkers.add(checker);
    }

    public void addDataSource(DataSource dataSource) {
        if (null != dataSource) this.instanceDataSources.add(dataSource);
    }

    public void release() {
        instanceDataSources.forEach(dataSource -> {
            if (dataSource instanceof Disposable) {
                try {
                    ((Disposable) dataSource).dispose();
                } catch (Exception e) {
                    logger.info("[release] dataSource release fail", e);
                }
            }
        });
    }

}
