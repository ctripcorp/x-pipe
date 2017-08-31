package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.api.organization.Organization;
import com.ctrip.xpipe.api.organization.OrganizationModel;

import com.ctrip.xpipe.spring.RestTemplateFactory;

import org.springframework.web.client.RestOperations;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zhuchen on 2017/8/30.
 */
public class CtripOrganizationService implements Organization {

    private CtripOrganizationConfig config = new CtripOrganizationConfig();

    RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();

    @Override
    public List<OrganizationModel> retrieveOrganizationInfo() {
        String accessToken = config.getCmsAccessToken();
        String url = config.getCmsOrganizationUrl();
        AccessBody accessBody = new AccessBody();
        accessBody.setAccess_token(accessToken);
        OrgResponseBody response = restTemplate.postForObject(url, accessBody, OrgResponseBody.class);
        List<OrganizationModel> orgs = response
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


    @Override public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
