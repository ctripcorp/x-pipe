package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Nov 30, 2016
 */
public enum META_SERVER_SERVICE {

    //common
    GET_ACTIVE_KEEPER(PATH.GET_ACTIVE_KEEPER, ForwardType.FORWARD),

    //console
    CLUSTER_CHANGE(PATH.PATH_CLUSTER_CHANGE, ForwardType.MULTICASTING),
    CHANGE_PRIMARY_DC_CHECK(PATH.PATH_CHANGE_PRIMARY_DC_CHECK, ForwardType.FORWARD),
    CHANGE_PRIMARY_DC(PATH.PATH_CHANGE_PRIMARY_DC, ForwardType.MULTICASTING),
    MAKE_MASTER_READONLY(PATH.PATH_MAKE_MASTER_READONLY, ForwardType.FORWARD),
    GET_CURRENT_MASTER(PATH.PATH_GET_CURRENT_MASTER, ForwardType.FORWARD),

    //keeper
    KEEPER_TOKEN_STATUS(PATH.KEEPER_TOKEN_STATUS, ForwardType.MULTICASTING),

    //multi dc
    UPSTREAM_CHANGE(PATH.PATH_UPSTREAM_CHANGE, ForwardType.FORWARD),
    GET_PEER_MASTER(PATH.GET_PEER_MASTER, ForwardType.FORWARD),
    UPSTREAM_PEER_CHANGE(PATH.PATH_UPSTREAM_PEER_CHANGE, ForwardType.FORWARD);

    private String path;
    private ForwardType forwardType;

    META_SERVER_SERVICE(String path, ForwardType forwardType) {
        this.path = path;
        this.forwardType = forwardType;
    }

    public String getPath() {
        return path;
    }

    public ForwardType getForwardType() {
        return forwardType;
    }

    public String getRealPath(String host) {

        if (!host.startsWith("http")) {
            host += "http://";
        }
        return String.format("%s%s%s", host, PATH.PATH_PREFIX, getPath());
    }


    protected boolean match(String realPath) {

        String[] realsp = StringUtil.splitRemoveEmpty("\\/+", realPath);
        String[] sp = StringUtil.splitRemoveEmpty("\\/+", getPath());

        if (sp.length != realsp.length) {
            return false;
        }
        for (int i = 0; i < sp.length; i++) {

            if (sp[i].startsWith("{")) {
                continue;
            }
            if (!realsp[i].equalsIgnoreCase(sp[i])) {
                return false;
            }
        }
        return true;
    }

    public static META_SERVER_SERVICE fromPath(String path) {
        return fromPath(path, PATH.PATH_PREFIX);
    }

    protected static META_SERVER_SERVICE fromPath(String path, String prefix) {

        List<META_SERVER_SERVICE> matched = new LinkedList<>();
        if (prefix != null && path.startsWith(prefix)) {
            path = path.substring(prefix.length());
        }

        for (META_SERVER_SERVICE service : META_SERVER_SERVICE.values()) {
            if (service.match(path)) {
                matched.add(service);
            }
        }
        if (matched.size() == 1) {
            return matched.get(0);
        }
        throw new IllegalStateException("from path:" + path + ", we found matches:" + matched);
    }

    public static class PATH extends AbstractController {

        public static final String PATH_PREFIX = "/api/meta";

        //common
        public static final String GET_ACTIVE_KEEPER = "/getactivekeeper/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE;

        //keeper
        public static final String KEEPER_TOKEN_STATUS = "/keeper/token/status";

        //console
        public static final String PATH_CLUSTER_CHANGE = "/clusterchange/" + CLUSTER_ID_PATH_VARIABLE;
        public static final String PATH_CHANGE_PRIMARY_DC_CHECK = "/changeprimarydc/check/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE + "/{newPrimaryDc}";
        public static final String PATH_CHANGE_PRIMARY_DC = "/changeprimarydc/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE + "/{newPrimaryDc}";
        public static final String PATH_MAKE_MASTER_READONLY = "/masterreadonly/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE + "/{readOnly}";
        public static final String PATH_GET_CURRENT_MASTER = "/getcurrentmaster/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE;

        //multi dc
        public static final String PATH_UPSTREAM_CHANGE = "/upstreamchange/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE + "/{ip}/{port}";
        public static final String GET_PEER_MASTER = "/getpeermaster/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE;
        public static final String PATH_UPSTREAM_PEER_CHANGE = "/upstreampeerchange/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE;

    }

}
