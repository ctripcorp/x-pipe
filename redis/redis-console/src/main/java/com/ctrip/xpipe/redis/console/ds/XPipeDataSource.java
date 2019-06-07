package com.ctrip.xpipe.redis.console.ds;

import com.ctrip.platform.dal.dao.configure.SwitchableDataSourceStatus;
import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSource;
import com.ctrip.platform.dal.dao.datasource.IForceSwitchableDataSource;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.lookup.annotation.Named;

import java.sql.Connection;
import java.sql.SQLException;

@Named(type = DataSource.class, value = "xpipe", instantiationStrategy = Named.PER_LOOKUP)
public class CtripDalBasedDataSource implements DataSource {


}
