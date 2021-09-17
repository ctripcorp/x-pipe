package com.ctrip.xpipe.testutils.stateful;

/**
 * @author Slight
 * <p>
 * Sep 17, 2021 10:47 PM
 */
public class StateHolder {

    private String state = "UNSET";

    public StateHolder(String state) {
        this.state = state;
    }

    public String state() {
        return state;
    }
}
