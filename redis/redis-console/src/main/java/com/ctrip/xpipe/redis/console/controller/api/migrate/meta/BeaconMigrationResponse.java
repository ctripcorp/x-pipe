package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class BeaconMigrationResponse {

    private static final int SUCCESS = 0;
    private static final int FAIL = 1;
    private static final int FAIL_NO_RETRY = -1;

    private int code;

    private String msg;

    public BeaconMigrationResponse() {

    }

    private BeaconMigrationResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public static BeaconMigrationResponse success() {
        return new BeaconMigrationResponse(SUCCESS, "success");
    }

    public static BeaconMigrationResponse fail(String err) {
        return new BeaconMigrationResponse(FAIL, err);
    }

    public static BeaconMigrationResponse skip(String err) {
        return new BeaconMigrationResponse(FAIL_NO_RETRY, err);
    }

    public boolean isSuccess() {
        return SUCCESS == this.code;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeaconMigrationResponse that = (BeaconMigrationResponse) o;
        return code == that.code &&
                Objects.equals(msg, that.msg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, msg);
    }
}
