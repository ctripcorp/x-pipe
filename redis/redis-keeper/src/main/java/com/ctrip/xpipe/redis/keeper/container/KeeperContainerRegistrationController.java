package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.cluster.LeaderLatchProbeStatus;
import com.ctrip.xpipe.cluster.RebuildableLeaderLatch.ManualRebuildResult;
import com.ctrip.xpipe.redis.keeper.container.resp.RestResp;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * keepercontainer ZK 地址注册的运维入口（keeper-dynamic-ip spec §5.6 Ih）。
 * <p>
 * 挂在 /keepers 下，随 {@link HostNameVerifyFilter}：请求带 {@code X-Keeper-Container-Host} 时校验目标
 * pod 身份，IP 被复用后不会误重置别的 pod。
 * <p>
 * 响应体沿用 {@link RestResp}：code 0 成功；{@link #CODE_REJECTED} 健康实例未带 force；
 * {@link #CODE_NOT_RUNNING} 注册未启动（非生产 profile 无注册器，或未 start / 已 stop）；
 * {@link #CODE_FAILED} 新实例 start 失败。
 */
@RestController
@RequestMapping("/keepers/registration")
public class KeeperContainerRegistrationController extends AbstractController {

    public static final int CODE_REJECTED = 409;
    public static final int CODE_FAILED = 500;
    public static final int CODE_NOT_RUNNING = 503;

    /** 注册器只在生产 profile 装配。 */
    @Autowired(required = false)
    private KeeperContainerZkRegistrar registrar;

    public KeeperContainerRegistrationController() {
    }

    @VisibleForTesting
    KeeperContainerRegistrationController(KeeperContainerZkRegistrar registrar) {
        this.registrar = registrar;
    }

    /**
     * 只读。返回最近一次断链探测结果（最多滞后一个探测间隔，默认 10s）：verdict / reason、latch 状态、
     * ourPath、节点是否存在、ephemeralOwner 与当前 sessionId、连续不健康时长。
     */
    @GetMapping
    public RestResp<LeaderLatchProbeStatus> status() {
        logger.info("[status]");
        LeaderLatchProbeStatus status = null == registrar ? null : registrar.probeStatus();
        if (null == status) {
            return RestResp.fail(CODE_NOT_RUNNING, "registration not running");
        }
        return RestResp.success(status);
    }

    /**
     * 手工重置注册：关闭当前 LeaderLatch、新建并 start，与自动重建同一套逻辑、同一把锁。
     * <p>
     * <b>误重置代价</b>：本 pod 的注册节点被删除并以更大 seq 重建，排到同 hostname 下其他 pod 之后。
     * 滚动替换期间新旧 pod 并存时，消费方会立即改为解析到另一个 pod（可能尚未就绪）。
     * 因此最近一次探测不是 UNHEALTHY（含尚未探测、连接不健康等无结论的情况）时拒绝执行，
     * 确认要重置健康实例须显式带 {@code force=true}。
     */
    @PostMapping("/reset")
    public RestResp<LeaderLatchProbeStatus> reset(@RequestParam(defaultValue = "false") boolean force) {
        logger.info("[reset]force:{}", force);
        if (null == registrar) {
            return RestResp.fail(CODE_NOT_RUNNING, "registration not running");
        }
        ManualRebuildResult result = registrar.reset(force, "http");
        switch (result) {
            case REBUILT:
                return RestResp.success(registrar.probeStatus());
            case REJECTED_NOT_UNHEALTHY:
                return RestResp.fail(CODE_REJECTED, "latest probe is not UNHEALTHY, add force=true to reset anyway; "
                        + "reset re-queues this pod after other pods of the same hostname");
            case NOT_RUNNING:
                return RestResp.fail(CODE_NOT_RUNNING, "registration not running");
            default:
                return RestResp.fail(CODE_FAILED, "rebuild started but new latch failed to start, see keeper log");
        }
    }
}
