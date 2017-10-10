package com.ctrip.xpipe.redis.console.build;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import java.util.ArrayList;
import java.util.List;

final class FxxpipeDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();


      defineSimpleTableProviderComponents(all, "fxxpipe", com.ctrip.xpipe.redis.console.model._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.xpipe.redis.console.model._INDEX.getDaoClasses());

      return all;
   }
}
