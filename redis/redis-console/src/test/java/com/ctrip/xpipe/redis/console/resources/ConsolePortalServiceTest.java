package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsolePortalServiceTest {

    @Mock
    private ConsoleConfig config;

    @InjectMocks
    private ConsolePortalService consoleService;

    @Mock
    private RestTemplate restTemplate;

    @Test
    public void testGetActiveRoutes() {
        // Arrange
        String url = "http://example.com/api/routes/active";
        String jsonResponse = "[ {\n" +
                "  \"id\" : 12,\n" +
                "  \"orgId\" : 0,\n" +
                "  \"clusterType\" : \"\",\n" +
                "  \"srcProxyIds\" : \"\",\n" +
                "  \"dstProxyIds\" : \"869,870\",\n" +
                "  \"optionProxyIds\" : \"\",\n" +
                "  \"srcDcName\" : \"SHAFQ\",\n" +
                "  \"dstDcName\" : \"FRA-AWS\",\n" +
                "  \"tag\" : \"console\",\n" +
                "  \"active\" : true,\n" +
                "  \"isPublic\" : true,\n" +
                "  \"description\" : \"---->21034014\"\n" +
                "}, {\n" +
                "  \"id\" : 22,\n" +
                "  \"orgId\" : 0,\n" +
                "  \"clusterType\" : \"\",\n" +
                "  \"srcProxyIds\" : \"914,915\",\n" +
                "  \"dstProxyIds\" : \"869,870\",\n" +
                "  \"optionProxyIds\" : \"\",\n" +
                "  \"srcDcName\" : \"SHARB\",\n" +
                "  \"dstDcName\" : \"FRA-AWS\",\n" +
                "  \"tag\" : \"console\",\n" +
                "  \"active\" : true,\n" +
                "  \"isPublic\" : true,\n" +
                "  \"description\" : \"[公共] xreg 21062164---->21061327\"\n" +
                "}]";
        ResponseEntity<String> responseEntity = ResponseEntity.ok(jsonResponse);

        when(config.getConsoleNoDbDomain()).thenReturn("http://example.com");
        when(restTemplate.exchange(url, HttpMethod.GET, null, String.class))
                .thenReturn(responseEntity);

        List<RouteModel> routeModels = consoleService.getActiveRoutes();
        for(RouteModel routeModel : routeModels) {
            System.out.println(routeModel.getDescription());
        }
        Assert.assertEquals(routeModels.size(), 2);
        Assert.assertEquals(routeModels.get(1).getId(), 22);

    }
}
