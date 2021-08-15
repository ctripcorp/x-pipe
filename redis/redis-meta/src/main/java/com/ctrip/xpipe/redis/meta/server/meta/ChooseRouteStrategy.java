package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;

import java.util.List;
import java.util.Random;

public interface ChooseRouteStrategy {
    RouteMeta choose(List<RouteMeta> routeMetas);
    public static ChooseRouteStrategy RANDOM = new RandomChooseRouteStrategy();
    static class HashCodeChooseRouteStrategy implements ChooseRouteStrategy {
        private int hashCode;
        public HashCodeChooseRouteStrategy(int code) {
            this.hashCode = code;
        }
        @Override
        public RouteMeta choose(List<RouteMeta> routeMetas) {
            if(routeMetas == null || routeMetas.isEmpty()) {
                return null;
            }
            return routeMetas.get(hashCode % routeMetas.size());
        }

        public void setCode(int hashCode) {
            this.hashCode = hashCode;
        }

        public int getCode() {
            return hashCode;
        }
    }

    class RandomChooseRouteStrategy implements  ChooseRouteStrategy {
        @Override
        public RouteMeta choose(List<RouteMeta> routeMetas) {
            if(routeMetas == null || routeMetas.isEmpty()) {
                return null;
            }
            int random = new Random().nextInt(routeMetas.size());
            return routeMetas.get(random);
        }
    }
}



