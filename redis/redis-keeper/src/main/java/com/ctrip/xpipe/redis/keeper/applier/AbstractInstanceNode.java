package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.server.AbstractServer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:19
 */
public abstract class AbstractInstanceNode extends AbstractServer {

    public List<AbstractInstanceComponent> components;

    public List<Object> dependencies;

    @Override
    public void initialize() throws Exception {

        components = new ArrayList();
        dependencies = new ArrayList<>();

        for (Field field : this.getClass().getFields()) {

            if (field.isAnnotationPresent(InstanceDependency.class)) {
                dependencies.add(field.get(this));
            }

            Object value = field.get(this);

            if (AbstractInstanceComponent.class.isInstance(value)) {
                components.add((AbstractInstanceComponent) value);
            }
        }

        for (AbstractInstanceComponent component : components) {
            component.inject(dependencies);
        }

        super.initialize();
    }

    @Override
    protected void doInitialize() throws Exception {
        for (AbstractInstanceComponent component : components) {
            component.initialize();
        }
    }

    @Override
    protected void doStart() throws Exception {
        for (AbstractInstanceComponent component : components) {
            component.start();
        }
    }

    @Override
    protected void doStop() throws Exception {
        for (AbstractInstanceComponent component : components) {
            component.stop();
        }
    }

    @Override
    protected void doDispose() throws Exception {
        for (AbstractInstanceComponent component : components) {
            component.dispose();
        }
    }
}
