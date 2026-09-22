package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

/**
 * slave 连到的 master 不在本 DC/集群/分片的 keeper 列表中（连错 keeper）。
 * 与「replId 不一致」区分：连错 keeper 需要主动拉出。
 */
public class KeeperNotInMetaException extends IllegalStateException {

    public KeeperNotInMetaException(String message) {
        super(message);
    }
}
