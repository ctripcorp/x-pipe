package com.ctrip.xpipe.redis.console.spring;

import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.ComponentDescriptor;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.unidal.lookup.ContainerLoader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class PlexusManualLoaderConfiguration {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Bean
    public PlexusContainer plexusContainer() throws Exception {


        DefaultPlexusContainer container = new DefaultPlexusContainer();

        List<ComponentDescriptor<?>> descriptors = scanAndRegisterComponents(container, "META-INF/plexus/*.xml");
        for (ComponentDescriptor<?> descriptor : descriptors) {
            container.addComponentDescriptor(descriptor);
        }

        postProcessAndInject(container, descriptors);

        return container;

    }

    private List<ComponentDescriptor<?>> scanAndRegisterComponents(DefaultPlexusContainer container, String resourcePattern) throws Exception {
        List<ComponentDescriptor<?>> result = new ArrayList<>();

        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath*:" + resourcePattern);

        if (resources.length == 0) {
            logger.info("Warning: No Plexus component descriptors found for pattern {}", resourcePattern);
            return null;
        }

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        for (Resource resource : resources) {
            if(resource.getURL().toString().contains("foundation-service") ||
                    resource.getURL().toString().contains("apollo-client")) {
                continue;
            }
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

                    result.add(descriptor);
                }
            }
        }
        return result;
    }

    private void postProcessAndInject(PlexusContainer container, List<ComponentDescriptor<?>> descriptors) {
        for (ComponentDescriptor<?> descriptor : descriptors) {

                // 通过lookup来实例化组件（如果尚未实例化）
            Object componentInstance = null;
            try {
                componentInstance = container.lookup(descriptor.getRole(), descriptor.getRoleHint());
            } catch (ComponentLookupException e) {
                throw new RuntimeException(e);
            }
            logger.debug("Processing component: {}", componentInstance.getClass().getName());

                // 手动注入依赖
            injectDependencies(container, componentInstance);

        }
    }

    private void injectDependencies(PlexusContainer container, Object component) {
        for (Field field : component.getClass().getDeclaredFields()) {
            // 寻找我们关心的 "遗留" 注解
            if (field.isAnnotationPresent(org.unidal.lookup.annotation.Inject.class)) {
                try {
                    field.setAccessible(true);

                    // 从注解中获取信息（这里简化处理，直接用字段类型作为role）
                    // Unidal的@Inject注解还可以指定role和roleHint，这里需要更复杂的逻辑来处理
                    Class<?> dependencyType = field.getType();

                    // 从容器中查找依赖
                    Object dependency = container.lookup(dependencyType);

                    // 反射注入
                    field.set(component, dependency);
                    logger.info("Successfully injected dependency of type {} into field {} of component {}",
                            dependencyType.getSimpleName(), field.getName(), component.getClass().getSimpleName());

                } catch (Exception e) {
                    logger.error("Failed to inject dependency for field {} in component {}", field.getName(), component.getClass().getName(), e);
                    // 注入失败，抛出异常或继续，取决于你的健壮性要求
                    throw new RuntimeException("Dependency injection failed", e);
                }
            }
        }
    }
}
