package com.ctrip.xpipe.redis.keeper.util;

import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.log.MDCUtil;

import java.util.concurrent.ThreadFactory;

/**
 * @author lishanglin
 * date 2023/10/31
 */
public class KeeperReplIdAwareThreadFactory extends XpipeThreadFactory {

    private String replId;

    public static ThreadFactory create(String replId, String namePrefix, boolean daemon) {
        return new KeeperReplIdAwareThreadFactory(replId, namePrefix, daemon);
    }

    public static ThreadFactory create(String replId, String namePrefix) {
        return create(replId, namePrefix, false);
    }

    public static ThreadFactory create(Object replId, String namePrefix) {
        String replIdString = null;
        if (replId != null) {
            replIdString = replId.toString();
        }
        return create(replIdString, namePrefix, false);
    }

    private KeeperReplIdAwareThreadFactory(String replId, String namePrefix, boolean daemon) {
        super(namePrefix, daemon);
        this.replId = replId;
    }

    private KeeperReplIdAwareThreadFactory(String replId, String namePrefix) {
        this(replId, namePrefix, false);
    }

    @Override
    public Thread newThread(Runnable r) {
        return super.newThread(MDCUtil.decorateKeeperReplMDC(r, replId));
    }

}
