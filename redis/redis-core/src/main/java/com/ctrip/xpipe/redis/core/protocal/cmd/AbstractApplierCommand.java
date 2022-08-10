package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.ApplierState;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 15:38
 */
public abstract class AbstractApplierCommand<T> extends AbstractRedisCommand<T> {

    public static String GET_STATE = "getstate";

    public static String SET_STATE = "setstate";

    public AbstractApplierCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public AbstractApplierCommand(ApplierMeta applierMeta, ScheduledExecutorService scheduled){
        super(applierMeta.getIp(), applierMeta.getPort(), scheduled);
    }

    @Override
    public String getName() {
        return "applier";
    }

    public static class ApplierGetStateCommand extends AbstractApplierCommand<ApplierState>{

        public ApplierGetStateCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        public ApplierGetStateCommand(ApplierMeta applierMeta, ScheduledExecutorService scheduled) {
            super(applierMeta, scheduled);
        }

        @Override
        protected ApplierState format(Object payload) {
            return ApplierState.valueOf(payloadToString(payload));
        }

        @Override
        public ByteBuf getRequest() {
            return new RequestStringParser(getName(), GET_STATE).format();
        }
    }

    public static class ApplierSetStateCommand extends AbstractApplierCommand<String>{

        private ApplierState state;
        private Pair<String, Integer> masterAddress;
        private String sids;
        private String gtidSet;
        private RouteMeta routeMeta;

        public ApplierSetStateCommand(SimpleObjectPool<NettyClient> clientPool,
                                     ApplierState state,
                                     Pair<String, Integer> masterAddress,
                                     String sids,
                                     String gtidSet,
                                     RouteMeta routeMeta,
                                     ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
            this.state = state;
            this.masterAddress = masterAddress;
            this.sids = sids;
            this.gtidSet = gtidSet;
            this.routeMeta = routeMeta;
        }

        @Override
        protected String format(Object payload) {

            return payloadToString(payload);
        }

        @Override
        public ByteBuf getRequest() {

            //TODO ayq delete
            GtidSet gtidSetToBeSend = new GtidSet(gtidSet);
            Set<String> sidSet = new HashSet<>(Arrays.asList(sids.split(",")));
            GtidSet filteredGtidSet = gtidSetToBeSend.filterGtid(sidSet);
            for (String sid : sidSet) {
                if (filteredGtidSet.getUUIDSet(sid) == null) {
                    filteredGtidSet.add(sid+":0");
                }
            }
            return new RequestStringParser(
                    getName(),
                    SET_STATE,
                    state.toString(),
                    masterAddress.getKey(), String.valueOf(masterAddress.getValue()),
                    filteredGtidSet.toString(),
                    routeMeta == null?"":(routeMeta.routeProtocol() + " " + ProxyEndpoint.PROXY_SCHEME.TCP.name())
            ).format();

            //TODO ayq open
//            return new RequestStringParser(
//                    getName(),
//                    SET_STATE,
//                    state.toString(),
//                    masterAddress.getKey(), String.valueOf(masterAddress.getValue()),
//                    sids, gtidSet,
//                    routeMeta == null?"":(routeMeta.routeProtocol() + " " + ProxyEndpoint.PROXY_SCHEME.TCP.name())
//            ).format();
        }


        @Override
        public String toString() {
            return String.format("(to:%s) %s %s %s %s %s %s %s", getClientPool().desc(), getName(), SET_STATE,
                    state.toString(), masterAddress.getKey(), masterAddress.getValue(), sids, gtidSet);
        }
    }
}
