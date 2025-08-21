package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.exception.ProxyProtocolParseException;
import com.ctrip.xpipe.redis.core.proxy.parser.content.CompressParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.DefaultProxyContentParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.ProxyContentParser;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestForAbstractProxyProtocolParser extends AbstractTest {

    private AbstractProxyProtocolParser parser = new DefaultProxyConnectProtocolParser();

    @Test
    public void testGetProxyOptionParser() {
    }

    @Test
    public void testFormat() {
        parser.getParsers().add(new DefaultProxyContentParser().setSubOptionParser(new CompressParser().setAlgorithm(new CompressAlgorithm() {
            @Override
            public String version() {
                return "1.0";
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.ZSTD;
            }
        })).setType(ProxyContentParser.ContentType.COMPRESS));
        ByteBuf byteBuf = parser.format();
        SimpleStringParser simpleStringParser = new SimpleStringParser();
        RedisClientProtocol<String> clientProtocol = simpleStringParser.read(byteBuf);
        String protocol = clientProtocol.getPayload();
        logger.info("{}", protocol);
        Assert.assertTrue(protocol.contains("#"));
    }

    @Test(expected = ProxyProtocolParseException.class)
    public void testRead() {
        parser.read("PROXY ROUTE PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390#;FORWARD_FOR 10.5.109.209:41085;SECURITY #;");
    }

    @Test
    public void removeKeyWord() {
    }

    @Test
    public void getParsers() {
    }

    @Test
    public void newProxyProtocol() {
    }

    @Test
    public void validate() {

    }
}