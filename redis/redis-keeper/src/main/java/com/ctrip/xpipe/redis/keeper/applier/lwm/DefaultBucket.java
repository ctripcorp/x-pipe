package com.ctrip.xpipe.redis.keeper.applier.lwm;

import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;

/**
 * @author Slight
 * <p>
 * Jun 03, 2022 16:40
 */
public class DefaultBucket implements Bucket {

    private volatile long lwm;

    protected ArrayList<Long> list;

    public DefaultBucket() {
        list = new ArrayList<>(256);
    }

    //Valid:
    //Every time when add() is invoked(), list[0] must be filled, representing low water mark,
    // list[1] must be larger than list[0]+1, indicating a gap.
    // Take [3,5,6] as an example, list[0] is filled, 3 is the low water mark, list[1] is
    // larger than list[0]+1 (5 > (3+1)). So we say [3, 5, 6] is valid after add() is called.
    //
    //Invalid:
    //Take [3,4,6,8] as an example, list[1] which is 4 is not larger than 3+1. It is invalid.
    // As 4 is seen as the lwm, 3 should be removed from the list, and the valid state should
    // be [4,6,8].
    @Override
    public synchronized Pair<Long, Boolean> add(long water) {
        int size = list.size();
        if (size == 0) {
            list.add(0, water);
            lwm = water;
            return Pair.from(water, true);
        }
        if (water < list.get(0)) {
            return Pair.from(list.get(0), false);
        }
        int index = search(water, 0, size);
        if (list.size() == index) {
            list.ensureCapacity(index + 1);
            list.add(index, water);
        }
        if (water != list.get(index)) {
            list.ensureCapacity(index + 1);
            list.add(index, water);
        }
        long newLwm = shiftTilLwm();

        Pair<Long, Boolean> result = Pair.from(newLwm, false);
        if (newLwm > lwm) {
            lwm = newLwm;
            result.setValue(true);
        }
        return result;
    }

    @Override
    public long lwm() {
        return lwm;
    }

    //Take [0, 1, 4] as an example, head is 0 and tail is 3.
    public int search(long n, int head, int tail) {
        if (head == tail)
            return head;
        int mid = (head + tail) / 2;
        long midValue = list.get(mid);
        if (n <= midValue) {
            return search(n, head, mid);
        } else {
            return search(n, mid + 1, tail);
        }
    }

    public boolean isHeadLwm() {
        //And when isHeadLwm() is called, list.size() must be larger than 1 (>=2).
        return !(list.get(0) == list.get(1) - 1);
    }

    public long shiftTilLwm() {
        //As shiftTilLwm() is called at the bottom of add(),
        // here list.size() here must be larger than 0 (>=1)
        while ((list.size() > 1) && (!isHeadLwm())) {
            list.remove(0);
        }
        return list.get(0);
    }
}
