package com.ctrip.xpipe.redis.console.build;

import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.xpipe.redis.console.ds.XpipeDataSourceProvider;

import java.util.ArrayList;
import java.util.List;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
    @Override
    public List<Component> defineComponents() {
        List<Component> all = new ArrayList<Component>();


        all.addAll(new FxxpipeDatabaseConfigurator().defineComponents());
        all.add(C(DataSourceProvider.class, XpipeDataSourceProvider.class)
                .config(E("datasourceFile").value("datasources.xml"),
                        E("baseDirRef").value("FXXPIPE_HOME")));
        return all;
    }

    public static void main(String[] args) {
        generatePlexusComponentsXmlFile(new ComponentsConfigurator());
    }
}
