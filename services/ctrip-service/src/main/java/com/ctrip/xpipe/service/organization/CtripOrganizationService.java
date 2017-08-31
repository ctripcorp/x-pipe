package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.organization.OrganizationModel;
import com.ctrip.xpipe.api.organization.OrganizationService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;

import com.ctrip.xpipe.spring.RestTemplateFactory;

import org.springframework.web.client.RestOperations;
import java.util.List;

/**
 * Created by zhuchen on 2017/8/30.
 */
public class CtripOrganizationService implements OrganizationService {

    private CtripOrganizationConfig config = new CtripOrganizationConfig();

    private RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();

    @Override
    public List<OrganizationModel> retrieveOrganizationInfo() {
        String accessToken = config.getCmsAccessToken();
        String url = config.getCmsOrganizationUrl();
        AccessBody accessBody = new AccessBody().setAccessToken(accessToken);
        ResponseEntity<OrgResponseBody> response = restTemplate.postForEntity(url, accessBody, OrgResponseBody.class);
        OrgResponseBody orgResponseBody = response.getBody();
        List<OrganizationModel> orgs = orgResponseBody
            .getData()
            .stream()
            .map(org->{
                OrganizationModel organizationModel = new OrganizationModel();
                organizationModel.setId(org.getOrganizationId());
                organizationModel.setName(org.getName());
                return organizationModel;
            }).sorted(new Comparator<OrganizationModel>() {
                @Override public int compare(OrganizationModel o1, OrganizationModel o2) {
                    long id1 = o1.getId(), id2 = o2.getId();
                    if(id1 > id2) {
                        return 1;
                    } else if(id1 < id2) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            })
            .collect(Collectors.toList());
        return orgs;
    }


}
