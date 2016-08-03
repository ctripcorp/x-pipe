package com.ctrip.xpipe.redis.console.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.framework.apollo.ds.ApolloDataSourceProvider;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();
      
      all.addAll(new XpipedemodbDatabaseConfigurator().defineComponents());
      all.add(A(ApolloDataSourceProvider.class));
      return all;
   }
}
