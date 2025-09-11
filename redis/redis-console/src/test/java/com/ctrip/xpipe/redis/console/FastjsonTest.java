package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.api.codec.Codec;

import java.util.ArrayList;
import java.util.List;

public class FastjsonTest {

    public static class Node {
        private String key;

        private String val;

        private int anInt;

        private long aLong;

        public Node(String key, String val, int anInt, long aLong) {
            this.key = key;
            this.val = val;
            this.anInt = anInt;
            this.aLong = aLong;
        }

        public Node(){}

        public String getKey() {
            return key;
        }

        public Node setKey(String key) {
            this.key = key;
            return this;
        }

        public String getVal() {
            return val;
        }

        public Node setVal(String val) {
            this.val = val;
            return this;
        }

        public int getAnInt() {
            return anInt;
        }

        public Node setAnInt(int anInt) {
            this.anInt = anInt;
            return this;
        }

        public long getaLong() {
            return aLong;
        }

        public Node setaLong(long aLong) {
            this.aLong = aLong;
            return this;
        }
    }

    static class NodeGroup {
        private List<Node> nodes = new ArrayList<>();

        public NodeGroup(List<Node> nodes) {
            this.nodes = nodes;
        }

        public List<Node> getNodes() {
            return nodes;
        }

        public NodeGroup setNodes(List<Node> nodes) {
            this.nodes = nodes;
            return this;
        }

        public NodeGroup addNode(Node node) {
            this.nodes.add(node);
            return this;
        }
    }


    public static void main(String[] args) {
        for(int i = 0; i < Integer.MAX_VALUE; i++) {
            Node node = new Node("hello"+i, "world"+i, i, i);
            byte[] serilized = Codec.DEFAULT.encodeAsBytes(node);
            Node deserilized = Codec.DEFAULT.decode(serilized, Node.class);
        }
    }
}
