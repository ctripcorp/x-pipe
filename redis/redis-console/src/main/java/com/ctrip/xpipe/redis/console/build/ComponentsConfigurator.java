package com.ctrip.xpipe.redis.console.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.xpipe.redis.console.dal.XpipeDalDataObjectAssembly;
import com.ctrip.xpipe.redis.console.dal.XpipeDalTransactionManager;
import com.ctrip.xpipe.redis.console.ds.XpipeDataSourceProvider;

/**
 * @author shyin
 *
 * Aug 26, 2016
 */
public class ComponentsConfigurator extends AbstractResourceConfigurator {
	
	public static final String KEY_XPIPE_LOCATION = "FXXPIPE_HOME";
	
    @Override
    public List<Component> defineComponents() {
        List<Component> all = new ArrayList<Component>();


        all.addAll(new FxxpipeDatabaseConfigurator().defineComponents());
        all.add(C(DataSourceProvider.class, XpipeDataSourceProvider.class)
                .config(E("datasourceFile").value("datasources.xml"),
                        E("baseDirRef").value(KEY_XPIPE_LOCATION)));
        all.add(A(XpipeDalTransactionManager.class));
        all.add(A(XpipeDalDataObjectAssembly.class));
        return all;
    }

    public static void main(String[] args) {
        generatePlexusComponentsXmlFile(new ComponentsConfigurator());
    }
}
