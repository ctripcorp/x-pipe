package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * keepercontainer 启动时把自身 hostname 与 IP 注册到本机房 ZK，供 console/metaserver/checker
 * 反查容器地址。节点路径 {registerPath}/{hostname}，数据带 IP。
 * <p>
 * 用临时节点：pod 销毁后 session 过期，映射自动失效，不需要谁来清理墓碑。代价是 session
 * 过期重连后必须重建 —— 与 watch 端不同，注册端的节点是自己创建的，没有别的来源能补回来。
 * <p>
 * 注册失败不阻塞进程启动。keeper 是数据面组件，承载着 redis 复制流，不该因为 ZK 抖动就起不来；
 * 若让启动失败，一次 ZK 故障会演变成整个机房 keepercontainer 的重启风暴。改为启动时尽力注册，
 * 之后由定时任务与连接状态回调持续自愈。
 * <p>
 * 只在启动时探测一次 IP，重建时复用该值：pod IP 与 pod 生命周期绑定，IP 变化必然伴随
 * pod 重建与进程重启，运行中不会变。
 * <p>
 * 注册策略：不删除其他进程的节点，避免"互删抖动"。如果发现节点已被其他 session 占用，
 * 打 warning 日志并等待对方 session 过期后由定时任务自动注册。正常发布场景下，
 * 旧 pod 被杀后 session 会在 40 秒内过期，新进程最多等待一个定时周期即可成功注册。
 */
public class KeeperContainerZkRegistrar extends AbstractLifecycle
        implements TopElement, ConnectionStateListener {

    private static final long DEFAULT_CHECK_INTERVAL_MILLI = 30 * 1000L;

    private final ZkClient zkClient;
    private final String hostName;
    private final String ip;
    private final long checkIntervalMilli;

    private ScheduledExecutorService scheduled;
    private ScheduledFuture<?> checkFuture;

    public KeeperContainerZkRegistrar(ZkClient zkClient, String hostName, String ip) {
        this(zkClient, hostName, ip, DEFAULT_CHECK_INTERVAL_MILLI);
    }

    public KeeperContainerZkRegistrar(ZkClient zkClient, String hostName, String ip, long checkIntervalMilli) {
        this.zkClient = zkClient;
        this.hostName = hostName;
        this.ip = ip;
        this.checkIntervalMilli = checkIntervalMilli;
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
        zkClient.get().getConnectionStateListenable().addListener(this);

        try {
            register();
        } catch (Throwable th) {
            logger.error("[doStart][register fail, will retry]{}", hostName, th);
        }

        scheduled = Executors.newScheduledThreadPool(1,
                XpipeThreadFactory.create("KeeperContainerZkRegistrar"));
        checkFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                registerIfAbsent();
            }
        }, checkIntervalMilli, checkIntervalMilli, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if (checkFuture != null) {
            checkFuture.cancel(true);
            checkFuture = null;
        }
        if (scheduled != null) {
            scheduled.shutdownNow();
            scheduled = null;
        }

        CuratorFramework client = zkClient.get();
        client.getConnectionStateListenable().removeListener(this);
        
        // 停止时只删除自己的节点
        try {
            String path = path();
            Stat stat = client.checkExists().forPath(path);
            if (stat != null) {
                long mySessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                if (stat.getEphemeralOwner() == mySessionId) {
                    client.delete().forPath(path);
                    logger.info("[doStop] deleted own node: {}", path);
                } else {
                    logger.info("[doStop] node owned by other session, skip deletion: {}", path);
                }
            }
        } catch (Throwable th) {
            // 停止时节点可能已随 session 消失，删不掉不影响停止流程
            logger.info("[doStop][delete fail]", th);
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        // CONNECTED 覆盖启动时 ZK 未就绪、连上后首次注册的情况；
        // RECONNECTED 覆盖 session 过期导致临时节点被删、需要重建的情况。
        if (ConnectionState.CONNECTED != newState && ConnectionState.RECONNECTED != newState) {
            return;
        }

        try {
            register();
        } catch (Throwable th) {
            logger.error("[stateChanged][{}][register fail, will retry]{}", newState, hostName, th);
        }
    }

    /**
     * 定时兜底：覆盖连接状态未变化但注册仍然缺失的场景（例如启动时 create 因偶发原因失败）。
     * 节点已在则不动，避免无谓地删建导致解析出现空窗。
     */
    private void registerIfAbsent() throws Exception {
        CuratorFramework client = zkClient.get();
        String path = path();
        
        if (null == client.checkExists().forPath(path)) {
            logger.info("[registerIfAbsent][node missing, re-register]{}", path());
            register();
            return;
        }
        
        // 节点存在，检查是否是自己的（应对 session 重连后节点仍在但 owner 变化的情况）
        long mySessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
        Stat stat = client.checkExists().forPath(path);
        if (stat != null && stat.getEphemeralOwner() != mySessionId) {
            logger.debug("[registerIfAbsent] node owned by other session, hostname={}, mySession={}, existingSession={}", 
                hostName, mySessionId, stat.getEphemeralOwner());
        }
    }

    private synchronized void register() throws Exception {
        String path = path();
        CuratorFramework client = zkClient.get();
        byte[] data = Codec.DEFAULT.encodeAsBytes(new KeeperContainerAddressInfo(ip));
        
        long mySessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
        
        Stat stat = client.checkExists().forPath(path);
        if (stat != null) {
            long existingSessionId = stat.getEphemeralOwner();
            
            if (existingSessionId == mySessionId) {
                // 自己的节点（session 重连），更新数据即可
                logger.info("[register] updating own node: {}", path);
                client.setData().forPath(path, data);
                return;
            }
            
            if (existingSessionId != 0) {
                // 其他进程的节点（正常发布时旧 session 未过期）
                // 不删除，等待对方 session 过期后由定时任务重新注册
                logger.warn("[register] node owned by other session, will register when old session expires. " +
                    "hostname={}, mySession={}, existingSession={}", hostName, mySessionId, existingSessionId);
                return;
            }
            
            // 持久节点（异常情况），删除
            logger.warn("[register] deleting persistent node: {}", path);
            client.delete().forPath(path);
        }
        
        // 创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
        logger.info("[register] {} -> {} (sessionId={})", path, ip, mySessionId);
    }

    private String path() {
        return MetaZkConfig.getKeeperContainerRegisterPath() + "/" + hostName;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
