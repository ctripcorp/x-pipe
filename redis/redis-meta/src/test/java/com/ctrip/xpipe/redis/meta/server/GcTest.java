package com.ctrip.xpipe.redis.meta.server;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 01, 2020
 */
public class GcTest {

    public static String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    private static final PersistEntityHodler persist = new PersistEntityHodler();

    private static final TemporaryEntityHolder temp = new TemporaryEntityHolder();

    public static void main(String[] args) {
        System.out.println("[start]" + currentTimeAsString());
        boolean persistMode = Boolean.parseBoolean(System.getProperty("persist.mode", "false"));
        makeCmsGenFull();
        int loopSize = Integer.parseInt(System.getProperty("loop.size", "20"));
        for (int i = 0; i < loopSize; i++) {
            allocForYGC();
            System.out.println("[temp-" + i + "]" + currentTimeAsString());
        }
        if (persistMode) {
            for (int i = 0; i < 2; i++) {
                makeCmsGenFull();
            }
            for (int i = 0; i < loopSize * 10; i++) {
                allocForYGC();
                System.out.println("[temp-" + i + "]" + currentTimeAsString());
            }
        }
        System.out.println("[end]" + currentTimeAsString());
    }

    private static void makeCmsGenFull() {
        int assumedOldGenSize = 10 * 1024 * 1024;
        int almostFullAllocs = assumedOldGenSize / Entity.allocSize - 5;
        System.out.println("[makeCmsGenFull] assumedOldGenSize: " + assumedOldGenSize + ", almostFullAllocs: " + almostFullAllocs);
        for (int i = 0; i < almostFullAllocs; i++) {
            persist.add(new Entity());
            System.out.println("[persist-" + i + "]" + currentTimeAsString());
        }
    }

    private static void allocForYGC() {
        temp.add(new Entity());
    }

    public static class Entity {

        private static final int allocSize = Integer.parseInt(System.getProperty("alloc.size", "1024"));

        private final byte[] mem;

        public Entity() {
            this.mem = new byte[allocSize];
        }
    }

    public static class TemporaryEntityHolder extends AbstractSet<Entity> implements Set<Entity> {

        @Override
        public Iterator<Entity> iterator() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean add(Entity entity) {
            return true;
        }
    }

    public static class PersistEntityHodler extends AbstractSet<Entity> implements Set<Entity> {

        private List<Entity> entities = new ArrayList<>();

        @Override
        public Iterator<Entity> iterator() {
            return entities.iterator();
        }

        @Override
        public int size() {
            return entities.size();
        }

        @Override
        public boolean add(Entity entity) {
            return entities.add(entity);
        }

        @Override
        public void clear() {
            System.out.println("[clear]" + currentTimeAsString());
            entities.clear();
        }
    }

    public static String currentTimeAsString() {
        return FastDateFormat.getInstance(format).format(new Date());
    }
}
