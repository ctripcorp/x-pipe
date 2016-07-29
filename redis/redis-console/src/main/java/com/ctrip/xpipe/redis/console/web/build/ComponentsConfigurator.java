package com.ctrip.xpipe.redis.console.web.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();
      
      all.addAll(new XpipedemodbDatabaseConfigurator().defineComponents());

      all.add(defineJdbcDataSourceConfigurationManagerComponent("/opt/ctrip/data/xpipe/datasources.xml"));
      return all;
   }

// tmp for package
//   public static void main(String[] args) {
//      generatePlexusComponentsXmlFile(new ComponentsConfigurator());
//   }
}
