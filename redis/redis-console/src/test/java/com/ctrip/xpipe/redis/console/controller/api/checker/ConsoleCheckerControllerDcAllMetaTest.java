package com.ctrip.xpipe.redis.console.controller.api.checker;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.controller.config.LZ4CompressionResponseBodyAdvice;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import net.jpountz.lz4.LZ4Factory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

/**
 * Phase CS / AC-13: {@code GET /api/meta/{dcName}/all} 改返 {@code byte[]} 以启用 LZ4。
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleCheckerControllerDcAllMetaTest extends AbstractConsoleTest {

    private static final String DC = "jq";

    @InjectMocks
    private ConsoleCheckerController controller;

    @Mock
    private MetaCache metaCache;

    @Mock
    private DcMetaService dcMetaService;

    private XpipeMeta cached;

    @Before
    public void setupConsoleCheckerControllerDcAllMetaTest() {
        cached = new XpipeMeta().addDc(new DcMeta(DC));
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(cached);
    }

    @After
    public void tearDownConsoleCheckerControllerDcAllMetaTest() {
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    public void testUncompressedBodyMatchesLegacyUtf8() {
        XpipeMeta expected = new XpipeMeta().addDc(new DcMeta(DC));
        byte[] xml = controller.getDcAllMeta(DC, "xml");
        byte[] json = controller.getDcAllMeta(DC, null);

        Assert.assertArrayEquals(expected.toString().getBytes(StandardCharsets.UTF_8), xml);
        Assert.assertArrayEquals(Codec.DEFAULT.encode(expected).getBytes(StandardCharsets.UTF_8), json);
    }

    @Test
    public void testLz4HeaderAndRoundtrip() throws Exception {
        byte[] uncompressed = controller.getDcAllMeta(DC, "xml");
        MethodParameter returnType = dcAllMetaReturnType();
        Assert.assertEquals(byte[].class, returnType.getParameterType());

        LZ4CompressionResponseBodyAdvice advice = new LZ4CompressionResponseBodyAdvice();
        bindRequest("lz4");
        Assert.assertTrue(advice.supports(returnType, ByteArrayHttpMessageConverter.class));

        HttpHeaders headers = new HttpHeaders();
        ServerHttpResponse response = Mockito.mock(ServerHttpResponse.class);
        Mockito.when(response.getHeaders()).thenReturn(headers);
        byte[] compressed = advice.beforeBodyWrite(uncompressed, returnType, MediaType.APPLICATION_OCTET_STREAM,
                ByteArrayHttpMessageConverter.class, Mockito.mock(ServerHttpRequest.class), response);

        Assert.assertEquals("lz4", headers.getFirst(HttpHeaders.CONTENT_ENCODING));
        byte[] decompressed = LZ4Factory.fastestInstance().safeDecompressor()
                .decompress(compressed, uncompressed.length);
        Assert.assertArrayEquals(uncompressed, decompressed);

        bindRequest(null);
        Assert.assertFalse(advice.supports(returnType, ByteArrayHttpMessageConverter.class));
    }

    @Test
    public void testDataSourceIsMetaCache() {
        controller.getDcAllMeta(DC, "xml");
        Mockito.verify(metaCache, Mockito.only()).getXpipeMeta();
        Mockito.verifyZeroInteractions(dcMetaService);
    }

    private MethodParameter dcAllMetaReturnType() throws NoSuchMethodException {
        Method method = ConsoleCheckerController.class.getMethod("getDcAllMeta", String.class, String.class);
        return new MethodParameter(method, -1);
    }

    private void bindRequest(String acceptEncoding) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader(HttpHeaders.ACCEPT_ENCODING)).thenReturn(acceptEncoding);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }
}
