package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.LeveledEmbededSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultLeveledEmbededSetTest {

    private DefaultLeveledEmbededSet<String> root;

    @Before
    public void beforeDefaultLeveledEmbededSetTest() {
        root = new DefaultLeveledEmbededSet<String>();
    }

    @Test
    public void getSuperSet() {
        Assert.assertNull(root.getSuperSet());
        Assert.assertNull(root.getSubSet());
        LeveledEmbededSet<String> level2 = root.getThrough(2);
        Assert.assertNotNull(root.getSubSet());
        Assert.assertNotNull(root.getSubSet().getSuperSet());
        Assert.assertNotNull(level2);
        Assert.assertNotNull(level2.getSuperSet());
    }

    @Test
    public void getSubSet() {
        DefaultLeveledEmbededSet<String> level1 = (DefaultLeveledEmbededSet<String>) root.getThrough(1);

        Assert.assertNotNull(level1);
        Assert.assertEquals(level1.getSuperSet(), root);
        Assert.assertEquals(root.getSubSet(), level1);
    }

    @Test
    public void getThrough() {
        DefaultLeveledEmbededSet<String> level1 = (DefaultLeveledEmbededSet<String>) root.getThrough(1);
        DefaultLeveledEmbededSet<String> level12 = (DefaultLeveledEmbededSet<String>) root.getThrough(1);
        Assert.assertSame(level1, level12);
    }

    @Test
    public void getCurrentSet() {
        root.add("hello");
        root.getThrough(1).getThrough(2).add("world");
        Assert.assertEquals(Sets.newHashSet("hello", "world"), root.getCurrentSet());

        Assert.assertEquals(Sets.newHashSet("world"), root.getSubSet().getCurrentSet());
        Assert.assertEquals(Sets.newHashSet("world"), root.getSubSet().getSubSet().getCurrentSet());
    }

    @Test
    public void add() {
        root.add("hello");
        Assert.assertEquals(Sets.newHashSet("hello"), root.getCurrentSet());
        root.add("hello");
        Assert.assertEquals(Sets.newHashSet("hello"), root.getCurrentSet());
    }

    @Test
    public void remove() {
        root.add("hello");
        Assert.assertEquals(Sets.newHashSet("hello"), root.getCurrentSet());
        root.remove("hello");
        Assert.assertEquals(Sets.newHashSet(), root.getCurrentSet());
        root.getThrough(10).add("hello, world");
        Assert.assertEquals(Sets.newHashSet("hello, world"), root.getCurrentSet());
        for(int i = 0; i < 10; i++) {
            Assert.assertEquals(Sets.newHashSet("hello, world"), root.getThrough(i).getCurrentSet());
        }
        root.getThrough(1).remove("hello, world");
        Assert.assertEquals(Sets.newHashSet(), root.getCurrentSet());
    }

    @Test
    public void resume() {
        root.add("hello");
        LeveledEmbededSet<String> level3 = root.getThrough(3);
        Assert.assertTrue(level3.getCurrentSet().isEmpty());
        root.resume("hello", 3);
        Assert.assertFalse(level3.getCurrentSet().isEmpty());
        Assert.assertEquals(Sets.newHashSet("hello"), level3.getCurrentSet());
        Assert.assertEquals(Sets.newHashSet("hello"), root.getCurrentSet());
    }

    /* Multi-thread test, no needed*/
}