package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.LongParser;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;

import java.util.Map;

/**
 * @author TB
 * <p>
 * 2025/11/10 13:52
 */
public class GtidxHandler extends AbstractCommandHandler {
    private Map<String, GtidxSection> sections = Maps.newConcurrentMap();

    public GtidxHandler(){
      register(new GtidxRemove());
      register(new GtidxAdd());
    }

    private void register(GtidxSection section) {
        sections.put(section.name().toLowerCase().trim(), section);
    }


    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) throws Exception {
        logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
        RedisKeeperServer redisKeeperServer = (RedisKeeperServer)redisClient.getRedisServer();
        ByteBuf result = doSectionHandler(args[0], args,redisKeeperServer);
        redisClient.sendMessage(result);
    }

    @Override
    public String[] getCommands() {
        return new String[]{"gtidx"};
    }

    private ByteBuf doSectionHandler(String section, String args[],RedisKeeperServer redisKeeperServer) throws Exception{
        GtidxSection gtidxSection = sections.get(section.toLowerCase().trim());
        if(gtidxSection == null){
            return new CommandBulkStringParser("ERR "+section+ " subcommand not supported!").format();
        }
        return gtidxSection.gtidx(args,redisKeeperServer);
    }

    private interface GtidxSection {
        ByteBuf gtidx(String args[], RedisKeeperServer redisKeeperServer) throws Exception;
        String name();
    }

    private abstract class AbstractGtidxSection implements GtidxSection{
        protected String validateArgs(String[] args, int minLength) {
            if (args.length < minLength) {
                return "ERR wrong number of arguments";
            }
            return null;
        }

        protected boolean isExecuted(String type) {
            return "executed".equalsIgnoreCase(type);
        }

        protected boolean isLost(String type) {
            return "lost".equalsIgnoreCase(type);
        }

        protected String validateType(String type) {
            if (!isExecuted(type) && !isLost(type)) {
                return "ERR type must be EXECUTED or LOST";
            }
            return null;
        }
    }

    private class GtidxRemove extends AbstractGtidxSection{

        @Override
        public ByteBuf gtidx(String args[],RedisKeeperServer redisKeeperServer) throws Exception {
            String result;
            result = validateArgs(args, 5);
            if(result != null){
                return new CommandBulkStringParser(result).format();
            }
            result = validateType(args[1]);
            if(result != null){
                return new CommandBulkStringParser(result).format();
            }
            GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());
            String uuid = args[2];
            long startGno = parseGno(args[3]);
            long endGno = parseGno(args[4]);

            if (startGno > endGno) {
                return new CommandBulkStringParser("ERR start_gno cannot be greater than end_gno").format();
            }

            gtidSet.compensate(uuid,startGno,endGno);
            if(isLost(args[1])) {
                ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
                MetaStore metaStore = replicationStore.getMetaStore();
                int removeCnt = metaStore.removeLost(gtidSet);
                return new LongParser(removeCnt).format();
            }
            return new CommandBulkStringParser("ERR only lost supported").format();
        }

        @Override
        public String name() {
            return "remove";
        }
    }

    private class GtidxAdd extends AbstractGtidxSection{

        @Override
        public ByteBuf gtidx(String args[],RedisKeeperServer redisKeeperServer) throws Exception {
            String result;
            result = validateArgs(args, 5);
            if(result != null){
                return new CommandBulkStringParser(result).format();
            }
            result = validateType(args[1]);
            if(result != null){
                return new CommandBulkStringParser(result).format();
            }
            GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());
            String uuid = args[2];
            long startGno = parseGno(args[3]);
            long endGno = parseGno(args[4]);

            if (startGno > endGno) {
                return new CommandBulkStringParser("ERR start_gno cannot be greater than end_gno").format();
            }

            gtidSet.compensate(uuid,startGno,endGno);
            if(isExecuted(args[1])) {
                ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
                MetaStore metaStore = replicationStore.getMetaStore();
                int addCount = metaStore.increaseExecuted(gtidSet);
                return new LongParser(addCount).format();
            }
            return new CommandBulkStringParser("ERR only lost supported").format();
        }

        @Override
        public String name() {
            return "add";
        }
    }


    private long parseGno(String gnoStr) {
        try {
            return Long.parseLong(gnoStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("ERR Invalid gno format: " + gnoStr);
        }
    }
}
