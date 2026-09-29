package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.RebuildableLeaderLatch;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * keepercontainer 启动时把自身 hostname 与 IP 注册到本机房 ZK，供 console/metaserver/checker
 * 反查容器地址。
 * <p>
 * 注册结构是两层（spec §5.5）：在 {@code {registerPath}/{hostname}} 下起一个 LeaderLatch，
 * latch 会创建 {@code _c_<uuid>-latch-<seq>} 临时顺序子节点，data 是 latch id，即
 * {@link KeeperContainerAddressInfo} JSON。hostname 由父路径承载，IP 由子节点 data 承载。
 * 同一 hostname 下有多个 pod 时，消费侧按 seq 取首节点（D-I1）。
 * <p>
 * 这里只把 LeaderLatch 当「有序临时注册」用，不看 hasLeadership，也不挂 leader 监听。
 * 选它是因为它只删自己的 ourPath（close / reset 都经 setNode），不会碰其他 session 的节点，
 * 新旧 pod 并存时不会互删。session 过期、重连后的重建也由 latch 自己完成，
 * 所以不再需要手写 session 归属判断、连接状态监听与定时补注册。
 * <p>
 * 注册失败不阻塞进程启动。keeper 是数据面组件，承载着 redis 复制流，不该因为 ZK 抖动就起不来；
 * 若让启动失败，一次 ZK 故障会演变成整个机房 keepercontainer 的重启风暴。
 * LeaderLatch.start() 本身不等连接：它在后台线程等连上后才建节点。
 * <p>
 * 只在启动时探测一次 IP，latch id 固定为该值：pod IP 与 pod 生命周期绑定，IP 变化必然伴随
 * pod 重建与进程重启，运行中不会变。
 * <p>
 * LeaderLatch 没有定时器兜底，回调遇到未处理的返回码会停摆（spec §5.6 的 A/B/C/D），
 * 所以只经 {@link RebuildableLeaderLatch} 持有，重建能力由它提供；断链探测在 Ig 接入。
 */
public class KeeperContainerZkRegistrar extends AbstractLifecycle implements TopElement {

    private final ZkClient zkClient;
    private final String hostName;
    private final String ip;

    private volatile RebuildableLeaderLatch latch;

    public KeeperContainerZkRegistrar(ZkClient zkClient, String hostName, String ip) {
        this.zkClient = zkClient;
        this.hostName = hostName;
        this.ip = ip;
    }

    @Override
    protected void doInitialize() throws Exception {
        // 配置层面的错误 fail fast：hostname 缺失会让节点无法被反查，IP 缺失会让消费方
        // 解析到空地址，两者都是静默故障。这与 ZK 暂时不可用是两类问题，后者不阻塞启动。
        if (StringUtil.isEmpty(hostName)) {
            throw new IllegalStateException("[KeeperContainerZkRegistrar]blank hostname");
        }
        if (StringUtil.isEmpty(ip)) {
            throw new IllegalStateException("[KeeperContainerZkRegistrar]blank ip, hostname:" + hostName);
        }
    }

    @Override
    protected void doStart() throws Exception {
        String path = path();
        String id = Codec.DEFAULT.encode(new KeeperContainerAddressInfo(ip));
        RebuildableLeaderLatch current = new RebuildableLeaderLatch(zkClient.get(), path, id);
        // 先赋值再 start：start 即使抛异常，内部 latch 也可能已置为 STARTED，doStop 要能 close 掉它
        this.latch = current;
        try {
            current.start();
            logger.info("[doStart][register]{} -> {}", path, ip);
        } catch (Throwable th) {
            logger.error("[doStart][register fail]{} -> {}", path, ip, th);
        }
    }

    @Override
    protected void doStop() throws Exception {
        RebuildableLeaderLatch current = this.latch;
        this.latch = null;
        if (null == current) {
            return;
        }
        // close 只删自己的 ourPath（后台 guaranteed delete），不动同 hostname 下其他 pod 的节点
        try {
            current.close();
            logger.info("[doStop][unregister]{}", path());
        } catch (Throwable th) {
            // 停止时节点可能已随 session 消失，或 latch 未成功 start，关不掉不影响停止流程
            logger.info("[doStop][close fail]{}", path(), th);
        }
    }

    /** 本进程 latch 子节点的完整路径，未建好或已关闭时为 null。 */
    @VisibleForTesting
    protected String ourPath() {
        RebuildableLeaderLatch current = this.latch;
        return null == current ? null : current.getOurPath();
    }

    private String path() {
        return MetaZkConfig.getKeeperContainerRegisterPath() + "/" + hostName;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
