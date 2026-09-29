package com.ctrip.xpipe.redis.comparator.balance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * T-LB.1 测试用假实现：分配器可注入、单测不碰网络（D23）。
 */
public class FakeServerGroupProvider implements ServerGroupProvider {

    private volatile List<String> ciCodes = Collections.emptyList();

    private volatile RuntimeException failure;

    private int listCalls;

    public FakeServerGroupProvider setCiCodes(String... codes) {
        this.failure = null;
        this.ciCodes = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(codes)));
        return this;
    }

    public FakeServerGroupProvider fail(RuntimeException failure) {
        this.failure = failure;
        return this;
    }

    public int getListCalls() {
        return listCalls;
    }

    @Override
    public List<String> listCiCodes() {
        listCalls++;
        if (failure != null) {
            throw failure;
        }
        return ciCodes;
    }
}
