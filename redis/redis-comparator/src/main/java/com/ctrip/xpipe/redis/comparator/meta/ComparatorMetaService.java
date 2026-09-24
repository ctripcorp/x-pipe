package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 本机房 DcMeta 客户端（D21 / §4.5 / §4.6.2 / D22）。
 * HTTP 走 {@link AbstractService} 的 {@code RestOperations}（连接/读超时 + 重试），
 * 禁止自行创建无超时的 HTTP 客户端。无共享可变状态，判定方法无副作用。
 */
public class ComparatorMetaService extends AbstractService {

    public static final String FORMAT_XML = "xml";

    public static final String ACCEPT_ENCODING_LZ4 = "lz4";

    public static final String QUERY_FORMAT = "format";

    private final ComparatorConfig config;

    private final FoundationService foundation;

    public ComparatorMetaService(ComparatorConfig config) {
        this(config, FoundationService.DEFAULT);
    }

    public ComparatorMetaService(ComparatorConfig config, FoundationService foundation) {
        this.config = config;
        this.foundation = foundation;
    }

    @VisibleForTesting
    ComparatorMetaService(ComparatorConfig config, FoundationService foundation,
                          int retryTimes, int retryIntervalMilli, int connectTimeout, int soTimeout) {
        super(retryTimes, retryIntervalMilli, connectTimeout, soTimeout);
        this.config = config;
        this.foundation = foundation;
    }

    /**
     * 拉本机房 DcMeta：{@code GET /api/meta/{dcName}/all?format=xml}，dcName 取
     * {@link FoundationService#getDataCenter()}。请求头 {@code Accept-Encoding: lz4}，
     * 解压由 {@code RestTemplateFactory} 已挂的 {@code LZ4DecompressionInterceptor} 完成。
     */
    public DcMeta getCurrentDcMeta() throws SAXException, IOException {
        String dcName = foundation.getDataCenter();
        String console = config.getConsoleAddress();
        if (StringUtil.isEmpty(console)) {
            throw new IllegalStateException("console.address is empty");
        }
        if (StringUtil.isEmpty(dcName)) {
            throw new IllegalStateException("dataCenter is empty");
        }
        logger.info("[getCurrentDcMeta] dc={}, console={}", dcName, console);
        XpipeMeta xpipeMeta = fetchXpipeMeta(console, dcName);
        DcMeta dcMeta = xpipeMeta.getDcs() == null ? null : xpipeMeta.getDcs().get(dcName);
        if (dcMeta == null) {
            logger.error("[getCurrentDcMeta] dc not found in XpipeMeta, dc={}, console={}", dcName, console);
            throw new RedisRuntimeException("dc not found in XpipeMeta, dc=" + dcName);
        }
        return dcMeta;
    }

    /**
     * 每轮建一次：{@code DcMeta.getKeeperContainers().getId()} ↔ {@code KeeperMeta.getKeeperContainerId()}。
     * 禁止照抄 meta-server 的 {@code TfsKeeperUtils}（它依赖 {@code DcMetaCache}）。
     */
    public Map<Long, KeeperContainerMeta> indexKeeperContainers(DcMeta dcMeta) {
        if (dcMeta == null) {
            throw new IllegalArgumentException("dcMeta is null");
        }
        Map<Long, KeeperContainerMeta> index = new HashMap<>();
        List<KeeperContainerMeta> containers = dcMeta.getKeeperContainers();
        if (containers == null) {
            return index;
        }
        for (KeeperContainerMeta container : containers) {
            if (container == null || container.getId() == null) {
                continue;
            }
            index.put(container.getId(), container);
        }
        return index;
    }

    public boolean isTfsKeeper(KeeperMeta keeper, Map<Long, KeeperContainerMeta> containers) {
        if (keeper == null || keeper.getKeeperContainerId() == null || containers == null) {
            return false;
        }
        KeeperContainerMeta container = containers.get(keeper.getKeeperContainerId());
        return KeeperDiskTypeUtils.isTfs(container != null ? container.getDiskType() : null);
    }

    public List<KeeperMeta> listTfsKeepers(ShardMeta shard, Map<Long, KeeperContainerMeta> containers) {
        if (shard == null || shard.getKeepers() == null) {
            return Collections.emptyList();
        }
        List<KeeperMeta> tfsKeepers = new ArrayList<>();
        for (KeeperMeta keeper : shard.getKeepers()) {
            if (isTfsKeeper(keeper, containers)) {
                tfsKeepers.add(keeper);
            }
        }
        return tfsKeepers;
    }

    private XpipeMeta fetchXpipeMeta(String console, String dcName) throws SAXException, IOException {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_DC_ALL_META)
                .queryParam(QUERY_FORMAT, FORMAT_XML)
                .buildAndExpand(dcName);
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.ACCEPT_ENCODING, ACCEPT_ENCODING_LZ4);
        HttpEntity<?> entity = new HttpEntity<>(headers);
        ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(comp.toString(), HttpMethod.GET, entity, String.class);
        } catch (RuntimeException e) {
            logger.error("[getCurrentDcMeta] fetch failed, dc={}, console={}", dcName, console, e);
            throw e;
        }
        String raw = response.getBody();
        if (StringUtil.isEmpty(raw)) {
            logger.error("[getCurrentDcMeta] empty body, dc={}, console={}", dcName, console);
            throw new RedisRuntimeException("empty DcMeta body, dc=" + dcName);
        }
        return DefaultSaxParser.parse(raw);
    }

    @VisibleForTesting
    RestOperations restOperations() {
        return restTemplate;
    }
}
