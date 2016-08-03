package com.ctrip.xpipe.redis.console.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.framework.apollo.ds.ApolloDataSourceProvider;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();

      all.addAll(new FxxpipeDatabaseConfigurator().defineComponents());
      all.add(A(ApolloDataSourceProvider.class));
      return all;
   }
}
