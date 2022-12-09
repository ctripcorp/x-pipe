package com.ctrip.xpipe.redis.keeper.applier;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:56
 */
public class AbstractInstanceNodeTest {

    @Test
    public void testDI() throws Exception {

        TestServer server = new TestServer();
        server.initialize();

        assertEquals(((A) server.a).b, server.b);
        assertEquals(((A) server.a).c, server.c);
        assertEquals(((B) server.b).c, server.c);
        assertEquals(((C) server.c).b, server.b);
    }

    public interface IA {
    }

    public interface IB {
    }

    public interface IC {
    }

    public class A extends AbstractInstanceComponent implements IA {

        @InstanceDependency
        public IB b;

        @InstanceDependency
        public IC c;
    }

    public class B extends AbstractInstanceComponent implements IB {

        @InstanceDependency
        public IC c;
    }

    public class C extends AbstractInstanceComponent implements IC {

        @InstanceDependency
        public IB b;
    }

    public class TestServer extends AbstractInstanceNode {

        @InstanceDependency
        public IA a = new A();

        @InstanceDependency
        public IB b = new B();

        @InstanceDependency
        public IC c = new C();

        @Override
        public SERVER_ROLE role() {
            return SERVER_ROLE.UNKNOWN;
        }
    }
}