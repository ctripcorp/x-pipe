package com.ctrip.xpipe.redis.console.spring;

import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.ComponentDescriptor;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.configuration.PlexusConfiguration;
import org.codehaus.plexus.configuration.xml.XmlPlexusConfiguration;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
            return result;
        }

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        for (Resource resource : resources) {
            if(resource.getURL().toString().contains("foundation-service") ||
                    resource.getURL().toString().contains("apollo-client")) {
                continue;
            }
            logger.info("Parsing Plexus component descriptor: {}", resource.getURL());
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
                    NodeList configNodes = component.getElementsByTagName("configuration");
                    if (configNodes.getLength() > 0) {
                        Element configElement = (Element) configNodes.item(0);
                        PlexusConfiguration componentConfiguration = new XmlPlexusConfiguration(toXpp3Dom(configElement));
                        descriptor.setConfiguration(componentConfiguration);
                        logger.info("Applied configuration for component role [{}], hint [{}]", role, roleHint);
                    }

                    result.add(descriptor);
                }
            }
        }
        return result;
    }

    /**
     * 辅助方法：将标准的 w3c.dom.Element 递归转换为 Plexus 的 Xpp3Dom 对象。
     * @param element The source DOM Element.
     * @return The converted Xpp3Dom object.
     */
    private Xpp3Dom toXpp3Dom(Element element) {
        Xpp3Dom dom = new Xpp3Dom(element.getTagName());

        // 1. 转换属性
        NamedNodeMap attributeNodes = element.getAttributes();
        for (int i = 0; i < attributeNodes.getLength(); i++) {
            Node attr = attributeNodes.item(i);
            dom.setAttribute(attr.getNodeName(), attr.getNodeValue());
        }

        // 2. 转换子节点和文本值
        NodeList childNodes = element.getChildNodes();
        StringBuilder textContent = new StringBuilder();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child instanceof Element) {
                // 如果子节点是元素，递归转换
                dom.addChild(toXpp3Dom((Element) child));
            } else if (child instanceof Text) {
                // 如果是文本节点，追加到内容中
                textContent.append(child.getNodeValue());
            }
        }

        // 3. 设置节点的值
        dom.setValue(textContent.toString().trim());

        return dom;
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

            // 注入配置值 - 这是新增的关键步骤
            injectConfiguration(componentInstance, descriptor.getConfiguration());

            // 手动注入依赖
            injectDependencies(container, componentInstance);
        }
    }

    /**
     * 注入组件配置值
     * @param component 组件实例
     * @param configuration Plexus配置对象
     */
    private void injectConfiguration(Object component, PlexusConfiguration configuration) {
        if (configuration == null) {
            return;
        }

        try {
            // 获取所有配置子节点
            PlexusConfiguration[] children = configuration.getChildren();

            for (PlexusConfiguration child : children) {
                String name = child.getName();
                String value = child.getValue();

                if (value != null && !value.trim().isEmpty()) {
                    // 将 kebab-case 转换为 camelCase 的 setter 方法名
                    String setterName = "set" + toCamelCase(name);

                    try {
                        // 查找并调用 setter 方法
                        Method setter = component.getClass().getMethod(setterName, String.class);
                        setter.invoke(component, value);
                        logger.info("Successfully set configuration property {} = {} for component {}",
                                component.getClass().getSimpleName(), name, value);
                    } catch (NoSuchMethodException e) {
                        logger.warn("Setter method for configuration property {} not found on component {}",
                                component.getClass().getSimpleName(), name);
                    } catch (Exception e) {
                        logger.error("Failed to set configuration property {} on component {}",
                                component.getClass().getSimpleName(), name, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to inject configuration into component {}", component.getClass().getName(), e);
        }
    }

    /**
     * 将 kebab-case 字符串转换为 camelCase
     * 例如：physical-table-name -> PhysicalTableName
     * @param kebabCase kebab-case 格式的字符串
     * @return camelCase 格式的字符串
     */
    private String toCamelCase(String kebabCase) {
        StringBuilder camelCase = new StringBuilder();
        boolean capitalizeNext = true;

        for (char c : kebabCase.toCharArray()) {
            if (c == '-') {
                capitalizeNext = true;
            } else if (capitalizeNext) {
                camelCase.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                camelCase.append(c);
            }
        }

        return camelCase.toString();
    }

    /**
     * 注入组件依赖，会递归向上查找父类中的 @Inject 字段
     * @param container Plexus 容器
     * @param component 组件实例
     */
    private void injectDependencies(PlexusContainer container, Object component) {
        // 从当前类开始，向上遍历继承树
        Class<?> currentClass = component.getClass();
        while (currentClass != null && currentClass != Object.class) {
            // 处理当前类中声明的字段
            for (Field field : currentClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(org.unidal.lookup.annotation.Inject.class)) {
                    try {
                        field.setAccessible(true);

                        // 如果字段已经被注入（可能在子类中被覆盖），则跳过
                        if (field.get(component) != null) {
                            continue;
                        }

                        Class<?> dependencyType = field.getType();
                        Object dependency = container.lookup(dependencyType);

                        field.set(component, dependency);
                        logger.info("Successfully injected dependency {} into field {} (declared in {}) of component {}",
                                component.getClass().getSimpleName(),
                                field.getName(),
                                currentClass.getSimpleName(),
                                dependencyType.getSimpleName());

                    } catch (Exception e) {
                        logger.error("Failed to inject dependency into field {} (declared in {}) of component {}",
                                component.getClass().getName(),
                                field.getName(),
                                currentClass.getSimpleName(), e);
                        throw new RuntimeException("Dependency injection failed", e);
                    }
                }
            }
            // 移动到父类，继续下一次循环
            currentClass = currentClass.getSuperclass();
        }
    }

}