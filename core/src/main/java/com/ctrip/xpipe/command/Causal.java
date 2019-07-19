package com.ctrip.xpipe.command;

public interface Causal<T, V> {

    V getCausation(T t);
}
