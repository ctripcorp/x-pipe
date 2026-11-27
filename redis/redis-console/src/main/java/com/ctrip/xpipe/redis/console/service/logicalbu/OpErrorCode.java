package com.ctrip.xpipe.redis.console.service.logicalbu;

/** Fixed error codes returned to the portal; {@link #message} is the short text the portal shows. */
public enum OpErrorCode {

    // CONSOLE
    BAD_REQUEST("请求参数错误"),
    UNSUPPORTED_OPERATION("不支持的操作"),
    LOGICAL_BU_NOT_FOUND("逻辑 BU 不存在"),
    OPERATION_CONFLICT("该逻辑 BU 已有任务在执行"),
    DATA_NOT_FOUND("元数据不存在"),
    PRIORITY_STALE("Priority 已被并发修改"),
    INTERNAL_ERROR("Console 内部错误"),

    // CONSOLE_DB
    DB_TIMEOUT("数据库访问超时"),
    DB_UNAVAILABLE("数据库连接失败"),
    DB_ERROR("数据库执行出错"),
    DB_WRITE_UNKNOWN("数据库写入结果未知，请人工确认 Priority"),

    // META_SERVER
    META_TIMEOUT("MetaServer 请求超时"),
    META_UNREACHABLE("MetaServer 无法连接"),
    META_HTTP_ERROR("MetaServer 返回错误"),
    META_ERROR("调用 MetaServer 失败"),
    META_RELOAD_NOT_APPLIED("MetaServer 未应用新元数据（刚有其他变更或变更过多），定时刷新后再生效"),
    META_RELOAD_UNSUPPORTED("MetaServer 版本不支持立即刷新，已改库，定时刷新（默认 60s）后生效"),

    // KEEPER
    KEEPER_TIMEOUT("KeeperContainer 请求超时"),
    KEEPER_UNREACHABLE("KeeperContainer 无法连接"),
    KEEPER_HTTP_ERROR("KeeperContainer 返回错误"),
    KEEPER_ERROR("调用 KeeperContainer 失败"),
    RECOVERY_REJECTED("Keeper 拒绝恢复请求");

    private final String message;

    OpErrorCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
