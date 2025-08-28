package com.ctrip.xpipe.api.utils;

import java.io.IOException;

@FunctionalInterface
public interface IOSupplier<T> {
    T get() throws IOException;
}
