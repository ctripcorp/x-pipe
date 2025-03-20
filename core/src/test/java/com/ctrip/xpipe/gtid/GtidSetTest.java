package com.ctrip.xpipe.gtid;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;

import static com.ctrip.xpipe.gtid.GtidSet.Interval;
import static com.ctrip.xpipe.gtid.GtidSet.UUIDSet;

public class GtidSetTest {

    private static final String UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    private static final String GTID_SET = UUID + ":1-15";

    private GtidSet gtidSet;

    @Before
    public void setUp() throws Exception {
        gtidSet = new GtidSet(GTID_SET);
    }

    @Test
    public void encode() throws IOException {
        byte[] decoded = gtidSet.encode();
        GtidSet clone = new GtidSet(Maps.newLinkedHashMap());
        clone.decode(decoded);
        Assert.assertEquals(clone, gtidSet);
    }

    @Test
    public void encodeMax() throws IOException {
        GtidSet gtidSet = new GtidSet(UUID + ":" + Long.MAX_VALUE);
        byte[] decoded = gtidSet.encode();
        GtidSet clone = new GtidSet(Maps.newLinkedHashMap());
        clone.decode(decoded);
        Assert.assertEquals(clone, gtidSet);
    }

    @Test
    public void testCompensate() {

        GtidSet result;

        //near
        result = new GtidSet("A:3-5:10-11");
        result.compensate("A", 1, 2);
        Assert.assertEquals(new GtidSet("A:1-5:10-11"), result);

        result = new GtidSet("A:3-5:10-11");
        result.compensate("A", 6, 9);
        Assert.assertEquals(new GtidSet("A:3-11"), result);

        result = new GtidSet("A:3-5:10-11");
        result.compensate("A", 12, 20);
        Assert.assertEquals(new GtidSet("A:3-5:10-20"), result);

        //overlap
        result = new GtidSet("A:3-6:10-13");
        result.compensate("A", 1, 5);
        Assert.assertEquals(new GtidSet("A:1-6:10-13"), result);

        result = new GtidSet("A:3-6:10-13");
        result.compensate("A", 5, 11);
        Assert.assertEquals(new GtidSet("A:3-13"), result);

        result = new GtidSet("A:3-6:10-13");
        result.compensate("A", 5, 13);
        Assert.assertEquals(new GtidSet("A:3-13"), result);

        result = new GtidSet("A:3-6:10-13");
        result.compensate("A", 11, 20);
        Assert.assertEquals(new GtidSet("A:3-6:10-20"), result);

        //all over
        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 3, 25);
        Assert.assertEquals(new GtidSet("A:3-25"), result);

        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 4, 24);
        Assert.assertEquals(new GtidSet("A:3-25"), result);

        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 2, 26);
        Assert.assertEquals(new GtidSet("A:2-26"), result);

        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 1, 27);
        Assert.assertEquals(new GtidSet("A:1-27"), result);

        //add
        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 7, 7);
        Assert.assertEquals(new GtidSet("A:3-7:10-13:20-25"), result);

        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 24, 24);
        Assert.assertEquals(new GtidSet("A:3-6:10-13:20-25"), result);

        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 26, 26);
        Assert.assertEquals(new GtidSet("A:3-6:10-13:20-26"), result);

        //from > to
        result = new GtidSet("A:3-6:10-13:20-25");
        result.compensate("A", 27, 26);
        Assert.assertEquals(new GtidSet("A:3-6:10-13:20-25"), result);
    }

    @Test
    public void compensateWithZero() {
        GtidSet gtidSet1 = new GtidSet(UUID + ":0");
        gtidSet1.compensate(UUID, 10, 20);
        Assert.assertEquals(new GtidSet(UUID+":10-20"), gtidSet1);
    }

    @Test
    public void testDistanceFrom() {
        Assert.assertEquals(2, new GtidSet("A:1-5").lwmDistance(new GtidSet("A:1-3")));
        Assert.assertEquals(12, new GtidSet("A:1-5,B:1-10").lwmDistance(new GtidSet("A:1-3")));
        Assert.assertEquals(2, new GtidSet("A:1-5").lwmDistance(new GtidSet("A:1-3,B:1-5")));

        Assert.assertEquals(12, new GtidSet("A:1-5,B:1-10").lwmDistance(new GtidSet("A:1-3,C:1-5")));
        Assert.assertEquals(7, new GtidSet("A:1-5,B:1-10").lwmDistance(new GtidSet("A:1-3,B:1-5")));
    }

    @Test
    public void testLwm() {
        Assert.assertEquals(5, new GtidSet("A:1-5").lwm("A"));
        Assert.assertEquals(0, new GtidSet("A:2-5").lwm("A"));
        Assert.assertEquals(1, new GtidSet("A:1").lwm("A"));
        Assert.assertEquals(0, new GtidSet("A:1").lwm("B"));
    }

    @Test
    public void testRise0() {

        GtidSet result;

        result = new GtidSet("A:3-5");
        Assert.assertEquals(0, result.rise("A:0"));
        Assert.assertEquals(new GtidSet("A:3-5"), result);

        result = new GtidSet("A:3-5");

        Assert.assertEquals(0, result.rise("B:5"));
        Assert.assertEquals(new GtidSet("A:3-5,B:1-5"), result);
    }

    @Test
    public void testRise1() {

        GtidSet result;

        result = new GtidSet("A:1-5");
        Assert.assertEquals(5, result.rise("A:5"));
        Assert.assertEquals(new GtidSet("A:1-5"), result);

        result = new GtidSet("A:1-5");
        Assert.assertEquals(5, result.rise("A:6"));
        Assert.assertEquals(new GtidSet("A:1-6"), result);

        result = new GtidSet("A:3-5");
        Assert.assertEquals(0, result.rise("A:7"));
        Assert.assertEquals(new GtidSet("A:1-7"), result);

        result = new GtidSet("A:3-5,B:5");
        Assert.assertEquals(0, result.rise("A:8"));
        Assert.assertEquals(new GtidSet("B:5,A:1-8"), result);
    }

    @Test
    public void testRise2() {

        GtidSet result;

        result = new GtidSet("A:5-10");
        Assert.assertEquals(0, result.rise("A:3"));
        Assert.assertEquals(new GtidSet("A:1-3:5-10"), result);

        result = new GtidSet("A:5-11");
        Assert.assertEquals(0, result.rise("A:4"));
        Assert.assertEquals(new GtidSet("A:1-11"), result);

        result = new GtidSet("A:5-12,B:5");
        Assert.assertEquals(0, result.rise("A:8"));
        Assert.assertEquals(new GtidSet("B:5,A:1-12"), result);
    }

    @Test
    public void testRise3() {

        GtidSet result;

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:3");
        Assert.assertEquals(new GtidSet("A:1-3:5-10:20-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:4");
        Assert.assertEquals(new GtidSet("A:1-10:20-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:5");
        Assert.assertEquals(new GtidSet("A:1-10:20-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:10");
        Assert.assertEquals(new GtidSet("A:1-10:20-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:11");
        Assert.assertEquals(new GtidSet("A:1-11:20-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:15");
        Assert.assertEquals(new GtidSet("A:1-15:20-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:19");
        Assert.assertEquals(new GtidSet("A:1-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:20");
        Assert.assertEquals(new GtidSet("A:1-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:25");
        Assert.assertEquals(new GtidSet("A:1-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:30");
        Assert.assertEquals(new GtidSet("A:1-30"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:31");
        Assert.assertEquals(new GtidSet("A:1-31"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:35");
        Assert.assertEquals(new GtidSet("A:1-35"), result);
    }

    @Test
    public void testRise4() {

        GtidSet result;

        result = new GtidSet("A:0");
        result.rise("A:29");
        Assert.assertEquals(new GtidSet("A:1-29"), result);

        result = new GtidSet("A:5-10:20-30");
        result.rise("A:29");
        Assert.assertEquals(new GtidSet("A:1-30"), result);
    }

    @Test
    public void testAdd() throws Exception {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:3-5");
        gtidSet.add("00000000-0000-0000-0000-000000000000:2");
        gtidSet.add("00000000-0000-0000-0000-000000000000:4");
        gtidSet.add("00000000-0000-0000-0000-000000000000:5");
        gtidSet.add("00000000-0000-0000-0000-000000000000:7");
        gtidSet.add("00000000-0000-0000-0000-000000000001:9");
        gtidSet.add("00000000-0000-0000-0000-000000000000:0");
        Assert.assertEquals(gtidSet.toString(),
                "00000000-0000-0000-0000-000000000000:0:2-5:7,00000000-0000-0000-0000-000000000001:9");
        Assert.assertEquals(0, gtidSet.lwm("00000000-0000-0000-0000-000000000000"));
        Assert.assertEquals(0, gtidSet.lwm("00000000-0000-0000-0000-000000000001"));
    }

    @Test
    public void testJoin() throws Exception {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:3-4:6-7");
        gtidSet.add("00000000-0000-0000-0000-000000000000:5");
        Assert.assertEquals(gtidSet.getUUIDSets().iterator().next().getIntervals().iterator().next().getEnd(), 7);
        Assert.assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:3-7");
    }

    @Test
    public void testEmptySet() throws Exception {
        Assert.assertEquals(new GtidSet("").toString(), "");
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(new GtidSet(""), new GtidSet(Maps.newLinkedHashMap()));
        Assert.assertEquals(new GtidSet(""), new GtidSet(""));
        Assert.assertEquals(new GtidSet(UUID + ":1-191"), new GtidSet(UUID + ":1-191"));
        Assert.assertEquals(new GtidSet(UUID + ":1-191:192-199"), new GtidSet(UUID + ":1-191:192-199"));
        Assert.assertEquals(new GtidSet(UUID + ":1-191:192-199"), new GtidSet(UUID + ":1-199"));
        Assert.assertEquals(new GtidSet(UUID + ":1-191:193-199"), new GtidSet(UUID + ":1-191:193-199"));
        Assert.assertNotEquals(new GtidSet(UUID + ":1-191:193-199"), new GtidSet(UUID + ":1-199"));
    }

    @Test
    public void testSubsetOf() {
        GtidSet[] set = {
                new GtidSet(""),
                new GtidSet(UUID + ":1-191"),
                new GtidSet(UUID + ":192-199"),
                new GtidSet(UUID + ":1-191:192-199"),
                new GtidSet(UUID + ":1-191:193-199"),
                new GtidSet(UUID + ":2-199"),
                new GtidSet(UUID + ":1-200")
        };
        byte[][] subsetMatrix = {
                {1, 1, 1, 1, 1, 1, 1},
                {0, 1, 0, 1, 1, 0, 1},
                {0, 0, 1, 1, 0, 1, 1},
                {0, 0, 0, 1, 0, 0, 1},
                {0, 0, 0, 1, 1, 0, 1},
                {0, 0, 0, 1, 0, 1, 1},
                {0, 0, 0, 0, 0, 0, 1},
        };
        for (int i = 0; i < subsetMatrix.length; i++) {
            byte[] subset = subsetMatrix[i];
            for (int j = 0; j < subset.length; j++) {
                Assert.assertEquals(set[i].isContainedWithin(set[j]), subset[j] == 1);
            }
        }
    }

    @Test
    public void testSingleInterval() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191");
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 1);
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1, 191));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-191");
    }

    @Test
    public void testCollapseAdjacentIntervals() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:192-199");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 1);
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(1, 199)));
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 199));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1, 199));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-199");
    }

    @Test
    public void testNotCollapseNonAdjacentIntervals() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:193-199");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 2);
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(193, 199));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-191:193-199");
    }

    @Test
    public void testMultipleIntervals() {
        GtidSet set = new GtidSet(UUID + ":1-191:193-199:1000-1033");
        UUIDSet uuidSet = set.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 3);
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(193, 199)));
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1000, 1033));
        Assert.assertEquals(set.toString(), UUID + ":1-191:193-199:1000-1033");
    }

    @Test
    public void testMultipleIntervalsThatMayBeAdjacent() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:192-199:1000-1033:1035-1036:1038-1039");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 4);
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(1000, 1033)));
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(1035, 1036)));
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 199));
        Assert.assertEquals(new LinkedList<GtidSet.Interval>(uuidSet.getIntervals()).getLast(), new Interval(1038, 1039));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-199:1000-1033:1035-1036:1038-1039");
    }

    @Test
    public void testPutUUIDSet() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191");
        GtidSet gtidSet2 = new GtidSet(UUID + ":1-190");
        UUIDSet uuidSet2 = gtidSet2.getUUIDSet(UUID);
        gtidSet.putUUIDSet(uuidSet2);
        Assert.assertEquals(gtidSet, gtidSet2);
    }

    @Test
    public void testClone() {
        GtidSet set = new GtidSet(UUID + ":1-191:193-199:1000-1033");
        GtidSet clone = set.clone();
        Assert.assertEquals(set, clone);
    }

    @Test
    public void testSame() {
        String gtidString = UUID + ":1";
        GtidSet set = new GtidSet(gtidString);
        String setString = set.toString();
        Assert.assertEquals(gtidString, setString);
    }

    @Test
    public void testSubtract() {
        GtidSet big = new GtidSet(UUID + ":1-10");
        GtidSet small = new GtidSet(UUID + ":3-8");
        GtidSet res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:9-10");

        big = new GtidSet(UUID + ":1-10:50-100");
        small = new GtidSet(UUID + ":3-8:75-85:98");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:9-10:50-74:86-97:99-100");
        res = small.subtract(big);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet(UUID + ":1-10:100");
        small = new GtidSet(UUID + ":100");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10");
        res = small.subtract(big);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet("");
        small = new GtidSet(UUID + ":3-8:75-85:98");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet(UUID + ":1-10:50-100");
        small = new GtidSet(UUID + ":3-8:75-85:98-99");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:9-10:50-74:86-97:100");

        big = new GtidSet(UUID + ":15-200");
        small = new GtidSet(UUID + ":3-50:100:150-180");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":51-99:101-149:181-200");

        big = new GtidSet(UUID + ":1-10:15-20");
        small = new GtidSet(UUID + ":3-12");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:15-20");

        big = new GtidSet(UUID + ":1-10:15-20");
        small = new GtidSet(UUID + ":3-17");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:18-20");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":3-17");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2");

        big = new GtidSet(UUID + ":1-10:15-20");
        small = new GtidSet(UUID + ":3-22");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42");
        small = new GtidSet(UUID + ":3-40");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:41-42");


        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":1-10");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":1-9");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":10");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":2-10");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":2-11");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet("");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10");

        String gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1:2172779-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1");

        gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");
    }

    @Test
    public void testReplace() {
        String uuid = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe";
        String gtidSetStringSlave = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172785,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringMaster = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-429";
        GtidSet slave = new GtidSet(gtidSetStringSlave);
        GtidSet master = new GtidSet(gtidSetStringMaster);
        GtidSet newSlave = slave.replaceGtid(master, uuid);
        Assert.assertEquals(newSlave.toString(), "e7d82d84-036c-11ea-bb09-075284a09713:1-427,56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782");

        newSlave = slave.replaceGtid(master, "");
        Assert.assertEquals(newSlave.toString(), slave.toString());
    }

    @Test
    public void testFilter() {
        Set<String> uuidSet = Sets.newHashSet("68226208-9374-11ea-819b-fa163e02998c", "02878c56-9375-11ea-b1c4-fa163eaa9d69", "dd3ccf94-9371-11ea-9f41-fa163ec90ff6");
        String gtidSetStringSlave = "02878c56-9375-11ea-b1c4-fa163eaa9d69:1-350744,106cab99-95c0-11ea-9ebe-fa163ec90ff6:1-4731";
        String gtidSetStringMaster = "68226208-9374-11ea-819b-fa163e02998c:1-349543,02878c56-9375-11ea-b1c4-fa163eaa9d69:1-371942,dd3ccf94-9371-11ea-9f41-fa163ec90ff6:1-580";
        GtidSet slave = new GtidSet(gtidSetStringSlave);
        GtidSet filtered = slave.filterGtid(uuidSet);
        GtidSet master = new GtidSet(gtidSetStringMaster);
        boolean res = filtered.isContainedWithin(master);
        Assert.assertEquals(res, true);
    }

    @Test
    public void test0to1() {
        GtidSet zero = new GtidSet("A:0");
        zero.add("A:1");
        Assert.assertEquals("A:1", zero.toString());
    }

    @Test
    public void test0toN() {
        GtidSet zero = new GtidSet("A:0");
        zero.add("A:5");
        Assert.assertEquals("A:5", zero.toString());
    }

    @Test
    public void test0raiseN() {
        GtidSet zero = new GtidSet("A:0");
        zero.rise("A:5");
        Assert.assertEquals("A:1-5", zero.toString());
    }

    @Test
    public void testIntersectionGtidSet() {
        String current = "24b9c5bc-070f-11ec-aa01-b8cef6507418:153148495-153445256,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1855122466-1855571751,c8331da3-512d-11e9-b435-48df3717a518:1068450202-1068695137,9c26dd63-3709-11ec-af66-1c34da7c121a:1881903-2107029,47e5a666-3708-11ec-895b-0c42a1002ff0:1747429-1969043";
        String executed = "34b4ecc5-3675-11ea-a598-b8599ffdbbb4:1-362422,5e279430-512e-11e9-b439-48df3717a524:1-128957648,9c26dd63-3709-11ec-af66-1c34da7c121a:1-13500738,c17c9fa6-c322-11e9-a0a2-98039bad5d88:1-13870,c200a3d7-3131-11ea-b1e7-e4434b6b0ae0:1-1198619808,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-1853256173:1853256176-1936745715,935066db-454b-11eb-bcfe-506b4b2af01e:1-271601355";
        GtidSet currentGtidSet = new GtidSet(current);
        GtidSet executeGtidSetd = new GtidSet(executed);
        Assert.assertEquals(currentGtidSet.isContainedWithin(executeGtidSetd), false);

        currentGtidSet = currentGtidSet.intersectionGtidSet(executeGtidSetd);
        Assert.assertEquals(currentGtidSet.isContainedWithin(executeGtidSetd), true);
    }

    @Test
    public void testIntersection() {
        /*
        GtidSet one = new GtidSet("A:1-10");
        GtidSet another = new GtidSet("A:5-12");

        Assert.assertEquals("A:5-10", one.intersectionGtidSet(another).toString());
        */
    }

    @Test
    public void testUnion() {

        GtidSet big = new GtidSet(UUID + ":1-10");
        GtidSet small = new GtidSet(UUID + ":1-12");
        GtidSet res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-12");

        big = new GtidSet(UUID + ":1-10:100");
        small = new GtidSet(UUID + ":3-8");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:100");

        small = new GtidSet(UUID + ":2-5");
        big = new GtidSet(UUID + ":7-15:100");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":2-5:7-15:100");

        big = new GtidSet(UUID + ":7-15:100");
        small = new GtidSet(UUID + ":2-5");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":2-5:7-15:100");

        big = new GtidSet(UUID + ":1-3:6-10:15-20");
        small = new GtidSet(UUID + ":17-50:100");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-3:6-10:15-50:100");

        big = new GtidSet(UUID + ":1-3:6-10:15-20:100");
        small = new GtidSet(UUID + ":17-50");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-3:6-10:15-50:100");

        big = new GtidSet(UUID + ":1-10:50-100:1000");
        small = new GtidSet(UUID + ":3-8:75-85:98-110:120-140");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:50-110:120-140:1000");

        big = new GtidSet(UUID + ":1-10:50-100:1000");
        small = new GtidSet(UUID + ":3-8:75-85:98-110:120-140");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-10:50-110:120-140:1000");

        big = new GtidSet(UUID + ":15-200:1000");
        small = new GtidSet(UUID + ":3-50:100:150-180");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":3-200:1000");

        big = new GtidSet(UUID + ":15-200");
        small = new GtidSet(UUID + ":3-50:100:150-180:1000");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":3-200:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":1-12");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-12:15-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":1-12");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-12:15-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-17");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-17");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-20:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":3-17");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-17:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":3-17");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-17:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-22");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-22:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-22");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-22:1000");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42:1000");
        small = new GtidSet(UUID + ":3-40");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-42:1000");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42:1000");
        small = new GtidSet(UUID + ":3-40");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-42:1000");


        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":1-10");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":1-9");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":2-10");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":2-11");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-11:1000");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":11-20");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-20");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet("");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet("");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet("");
        small = new GtidSet("");
        res = small.union(big);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912721");
        small = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-16504165:16504173-16541256:16541260-16545608:16545610-18913165");
        res = small.union(big);
        Assert.assertEquals(res.toString(), "cb190774-6bf1-11ea-9799-fa163e02998c:1-18913165");

        big = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912721,bb190774-6bf1-11ea-9799-fa163e02998c:1-18912");
        small = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912732,cb190774-6bf1-11ea-9799-fa163e029911:1-189127");
        res = small.union(big);
        Assert.assertEquals(res.toString(), "cb190774-6bf1-11ea-9799-fa163e02998c:1-18912732,cb190774-6bf1-11ea-9799-fa163e029911:1-189127,bb190774-6bf1-11ea-9799-fa163e02998c:1-18912");

        String gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.union(small);
        Assert.assertEquals(res.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");

        gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.union(small);
        Assert.assertEquals(res.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");
    }

    @Test
    public void testUnionWithZero() {
        GtidSet gtidSet1 = new GtidSet(UUID + ":0");
        GtidSet gtidSet2 = new GtidSet(UUID + ":1");
        Assert.assertEquals(new GtidSet(UUID + ":1"), gtidSet1.union(gtidSet2));

        gtidSet1 = new GtidSet(UUID + ":1-10");
        gtidSet2 = new GtidSet(UUID + ":0");
        Assert.assertEquals(new GtidSet(UUID + ":1-10"), gtidSet1.union(gtidSet2));

        gtidSet1 = new GtidSet(UUID + ":0");
        gtidSet2 = new GtidSet(UUID + ":0");
        Assert.assertEquals(new GtidSet(UUID + ":0"), gtidSet1.union(gtidSet2));
    }

    @Test
    public void testRetainAll() {
        GtidSet current = new GtidSet("a1:1-10:15-25,b1:1-100");
        GtidSet other = new GtidSet("a1:5-30,c1:1-100");
        Assert.assertEquals(new GtidSet("a1:5-10:15-25"), current.retainAll(other));
    }

    @Test
    public void testSymmetricDiff() {
        GtidSet self = new GtidSet("A:1-10:15-25,B:1-100");
        GtidSet other = new GtidSet("A:11-14,B:25-50,C:1-30:50-60");
        Assert.assertEquals(new GtidSet("A:1-25,B:1-24:51-100,C:1-30:50-60"), self.symmetricDiff(other));
        self = new GtidSet("A:1-100");
        other = new GtidSet("B:1-25");
        Assert.assertEquals(new GtidSet("A:1-100,B:1-25"), self.symmetricDiff(other));
        self = new GtidSet("B:1-15");
        other = new GtidSet("A:1-10,B:1-30,C:1-15");
        Assert.assertEquals(new GtidSet("A:1-10,B:16-30,C:1-15"), self.symmetricDiff(other));
        Assert.assertEquals(new GtidSet("A:1-10,B:16-30,C:1-15"), other.symmetricDiff(self));
    }

    @Test
    public void testItemCnt() {
        GtidSet gtidSet = new GtidSet("A:1-10:15-20:35-35,B:1-50,C:0");
        Assert.assertEquals(67, gtidSet.itemCnt());
    }

}