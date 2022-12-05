package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:29
 */
public class AbstractInstanceComponent extends AbstractLifecycle {

    public void inject(Map<String, Object> dependencies) throws IllegalAccessException {

        for (Field field : this.getClass().getFields()) {

            if (field.isAnnotationPresent(InstanceDependency.class)) {

                Class<?> type = field.getType();

                for (String name : dependencies.keySet()) {
                    Object dependency = dependencies.get(name);
                    if (name != null && name.equals(field.getName()) && type.isAssignableFrom(dependency.getClass())) {
                        field.set(this, dependency);

                        logger.info("[inject] ({}) as {} -> {} of ({})", dependency, name, field.getName(), this);
                    }
                }
            }
        }
    }
}
