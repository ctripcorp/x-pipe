package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.api.organization.Organization;
import com.ctrip.xpipe.api.organization.OrganizationModel;

import com.ctrip.xpipe.spring.RestTemplateFactory;

import org.springframework.web.client.RestOperations;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.monitor.CatTransactionMonitor.logger;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class CtripOrganizationService implements Organization {

    private CtripOrganizationConfig config = new CtripOrganizationConfig();

    RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();

    @Override
    public List<OrganizationModel> retrieveOrganizationInfo() {
        try {
            String accessToken = config.getCmsAccessToken();
            String url = config.getCmsOrganizationUrl();
            AccessBody accessBody = new AccessBody();
            accessBody.setAccess_token(accessToken);
            OrgResponseBody response = restTemplate.postForObject(url, accessBody, OrgResponseBody.class);
            if (!response.isStatus()) {
                logger.error("Could not get rest response from CMS system");
                logger.debug("{}", response);
            }
            List<OrganizationModel> orgs = response.getData().stream().map(org -> {
                OrganizationModel organizationModel = new OrganizationModel();
                organizationModel.setId(org.getOrganizationId());
                organizationModel.setName(org.getName());
                return organizationModel;
            }).sorted(new OrgIdComparator()).collect(Collectors.toList());
            return orgs;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }
    class OrgIdComparator implements Comparator<OrganizationModel> {
        @Override public int compare(OrganizationModel o1, OrganizationModel o2) {
            long id1 = o1.getId(), id2 = o2.getId();
            if (id1 > id2) {
                return 1;
            } else if (id1 < id2) {
                return -1;
            } else {
                return 0;
            }
        }
    }
    @Override public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
