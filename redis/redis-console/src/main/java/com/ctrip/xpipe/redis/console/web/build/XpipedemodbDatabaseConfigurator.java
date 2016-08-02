package com.ctrip.xpipe.redis.console.web.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class XpipedemodbDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();

      defineSimpleTableProviderComponents(all, "xpipedemodb", com.ctrip.xpipe.redis.console.web.model._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.xpipe.redis.console.web.model._INDEX.getDaoClasses());

      return all;
   }
}
