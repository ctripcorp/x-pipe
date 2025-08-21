package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Slight
 * <p>
 * Mar 05, 2022 10:59 AM
 */
public class AbstractMetaComparatorTest {

    @Test
    public void testCheckShallowChange() {
        ClusterMeta current = new ClusterMeta();
        ClusterMeta future = new ClusterMeta();

        MetaComparator comparator = new AbstractMetaComparator<ClusterMeta>() {

            @Override
            public void compare() {
                configChanged = checkShallowChange(current, future);
            }

            @Override
            protected void doDetailedCompare() {
                //do nothing
            }

            @Override
            public String idDesc() {
                return "Test";
            }
        };

        current.addShard(new ShardMeta());
        comparator.compare();
        assertFalse(comparator.isConfigChange());

        current.setDbId(10L);
        comparator.compare();
        assertTrue(comparator.isConfigChange());
    }

    @Test
    @Ignore
    public void testPerformance() {
        ClusterMeta current = new ClusterMeta();
        ClusterMeta future = new ClusterMeta();

        MetaComparator comparator = new AbstractMetaComparator<ClusterMeta>() {

            @Override
            public void compare() {
                configChanged = checkShallowChange(current, future);
            }

            @Override
            protected void doDetailedCompare() {
                //do nothing
            }

            @Override
            public String idDesc() {
                return "Test";
            }
        };

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            comparator.compare();
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("time elapsed: " + elapsed + "ms");
    }
}