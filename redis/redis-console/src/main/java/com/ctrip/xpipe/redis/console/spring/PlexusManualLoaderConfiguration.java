package com.ctrip.xpipe.redis.console.spring;

import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.ComponentDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;

@Configuration
public class PlexusManualLoaderConfiguration {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Bean
    public PlexusContainer plexusContainer() throws Exception {


        DefaultPlexusContainer container = new DefaultPlexusContainer();

        scanAndRegisterComponents(container, "META-INF/plexus/*.xml");

        return container;

    }

    private void scanAndRegisterComponents(DefaultPlexusContainer container, String resourcePattern) throws Exception {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath*:" + resourcePattern);

        if (resources.length == 0) {
            logger.info("Warning: No Plexus component descriptors found for pattern {}", resourcePattern);
            return;
        }

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        for (Resource resource : resources) {
            logger.info("Parsing Plexus component descriptor: {}" + resource.getURL());
            try (InputStream in = resource.getInputStream()) {
                Document doc = builder.parse(in);
                NodeList components = doc.getElementsByTagName("component");
                for (int i = 0; i < components.getLength(); i++) {
                    Element component = (Element) components.item(i);
                    String role = component.getElementsByTagName("role").item(0).getTextContent();
                    String roleHint = "default";
                    if (component.getElementsByTagName("role-hint").getLength() > 0) {
                        roleHint = component.getElementsByTagName("role-hint").item(0).getTextContent();
                    }
                    String implementation = component.getElementsByTagName("implementation").item(0).getTextContent();
                    ComponentDescriptor<?> descriptor = new ComponentDescriptor<>();
                    descriptor.setRole(role);
                    descriptor.setRoleHint(roleHint);
                    descriptor.setImplementation(implementation);

                    container.addComponentDescriptor(descriptor);
                }
            }
        }
    }

}
