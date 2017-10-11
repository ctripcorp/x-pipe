package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.ChooseNewMasterFailException;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 9, 2016
 */
public class FirstNewMasterChooserTest extends AbstractMetaServerTest {

    private List<RedisMeta> redises;

    private FirstNewMasterChooser firstNewMasterChooser;

    @Before
    public void beforeFirstNewMasterChooserTest() throws Exception {

        redises = new LinkedList<>();
        int port1 = randomPort();
        redises.add(new RedisMeta().setIp("localhost").setPort(port1));
        redises.add(new RedisMeta().setIp("localhost").setPort(randomPort(Sets.newHashSet(port1))));

        firstNewMasterChooser = new FirstNewMasterChooser(getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
    }

    @Test
    public void testChooseFirstAlive() throws Exception {

        try {
            firstNewMasterChooser.choose(redises);
            Assert.fail();
        }catch (ChooseNewMasterFailException e){
        }

        startSlaveFakeRedis(redises.get(1).getPort(), SERVER_ROLE.SLAVE);
        Assert.assertEquals(redises.get(1), firstNewMasterChooser.choose(redises));

        startSlaveFakeRedis(redises.get(0).getPort(), SERVER_ROLE.SLAVE);
        Assert.assertEquals(redises.get(0), firstNewMasterChooser.choose(redises));
    }

    @Test
    public void testSort() {

        int allCount = 5;

        List<RedisMeta> redisMetas = new LinkedList<>();
        for (int i = 0; i < allCount; i++) {
            redisMetas.add(new RedisMeta().setPort(i));
        }

        Assert.assertEquals(0, firstNewMasterChooser.sortAccording(redisMetas, new LinkedList<>()).size());

        List<RedisMeta> alive = new LinkedList<>();
        for (int i = 4; i >= 2; i--) {
            alive.add(redisMetas.get(i));
        }

        List<RedisMeta> sorted = firstNewMasterChooser.sortAccording(redisMetas, alive);

        Assert.assertEquals(3, sorted.size());

        RedisMeta previous = null;
        for(RedisMeta redis : sorted){
            if(previous != null){
                Assert.assertTrue(redis.getPort() > previous.getPort());
            }
            previous = redis;
        }
    }


    private Server startSlaveFakeRedis(Integer port, SERVER_ROLE serverRole) throws Exception {

        Role role = null;
        if (serverRole == SERVER_ROLE.MASTER) {
            role = new MasterRole();
        } else {
            role = new SlaveRole(serverRole, "localhost", port, MASTER_STATE.REDIS_REPL_CONNECT, 0L);
        }
        Server server = startServer(port, ByteBufUtils.readToString(role.format()));
        return server;
    }

    @Test
    public void testChooseExistingMaster() throws Exception {

        RedisMeta chosen = redises.get(1);
        startSlaveFakeRedis(chosen.getPort(), SERVER_ROLE.MASTER);
        Assert.assertEquals(chosen, firstNewMasterChooser.choose(redises));
    }

    @Test
    public void testMultiMaster() throws Exception {

        int addRedisCount = 5;
        Set<Integer> ports = new HashSet<>();
        redises.forEach(redisMeta -> ports.add(redisMeta.getPort()));

        for(int i=0;i<addRedisCount;i++){
            int port = randomPort(ports);
            redises.add(new RedisMeta().setIp("localhost").setPort(port));
        }

        for(RedisMeta redis : redises){
            startSlaveFakeRedis(redis.getPort(), SERVER_ROLE.MASTER);
        }
        try{
            RedisMeta choose = firstNewMasterChooser.choose(redises);
            Assert.fail();
        }catch (ChooseNewMasterFailException e){
            Assert.assertEquals(redises.size(), e.getRedises().size());
        }
    }

    //run with real redis
//	@Test
    public void testRedis() {

        redises.clear();
        redises.add(new RedisMeta().setIp("localhost").setPort(6379));
        redises.add(new RedisMeta().setIp("localhost").setPort(6479));
        logger.info("{}", firstNewMasterChooser.choose(redises));
    }
}
