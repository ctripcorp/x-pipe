package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:29
 */
public class AbstractInstanceComponent extends AbstractLifecycle {

    public void inject(List<Object> dependencies) throws IllegalAccessException {

        for (Field field : this.getClass().getFields()) {

            if (field.isAnnotationPresent(InstanceDependency.class)) {

                Class<?> type = field.getType();

                for (Object dependency : dependencies) {
                    if (type.isAssignableFrom(dependency.getClass())) {
                        field.set(this, dependency);
                    }
                }
            }
        }
    }
}
