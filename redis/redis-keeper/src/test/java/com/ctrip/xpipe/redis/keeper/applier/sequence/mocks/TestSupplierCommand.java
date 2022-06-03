package com.ctrip.xpipe.redis.keeper.applier.sequence.mocks;

import java.util.function.Supplier;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 10:08 PM
 */
public class TestSupplierCommand<V> extends AbstractThreadSwitchCommand<V> {

    private final Supplier<V> inner;

    public TestSupplierCommand(Supplier<V> inner) {
        this.inner = inner;
    }

    @Override
    public String getName() {
        return "TestSupplierCommand";
    }

    @Override
    protected void doBusiness() {
        V result = inner.get();
        if (result != null) {
            future().setSuccess(result);
        } else {
            future().setFailure(new RuntimeException("no result"));
        }
    }
}
