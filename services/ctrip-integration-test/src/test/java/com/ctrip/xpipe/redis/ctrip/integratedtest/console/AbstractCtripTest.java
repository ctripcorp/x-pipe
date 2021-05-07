package com.ctrip.xpipe.redis.ctrip.integratedtest.console;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.IOUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/4/21
 */
public class AbstractCtripTest extends AbstractMySqlTest {

    public static final String CONFIG_QCONFIG_DIR_PATH = "/opt/config/100004374/qconfig/";
    public static final String CONFIG_DAL_DIR_PATH = "/opt/config/100004374/dal/";

    public static final String CONFIG_QCONFIG_APPLICATION_PATH = "/opt/config/100004374/qconfig/application.properties";
    public static final String CONFIG_QCONFIG_DATASOURCE_PATH = "/opt/config/100004374/qconfig/datasource.properties";
    public static final String CONFIG_QCONFIG_DATASOURCES_PATH = "/opt/config/100004374/qconfig/datasources.xml";
    public static final String CONFIG_QCONFIG_SSO_CLIENT_PATH = "/opt/config/100004374/qconfig/sso_client_config.properties";
    public static final String CONFIG_QCONFIG_SSO_SERVICE_PATH = "/opt/config/100004374/qconfig/sso_service_address.properties";
    public static final String CONFIG_DAL_DATASOURCES_PATH = "/opt/config/100004374/dal/local-databases.properties";

    protected static final boolean REPLACE_CONFIG = Boolean.parseBoolean(System.getProperty("xpipe.config.replace", "true"));

    private static List<FileContentRegister> replacedConfigFiles;

    @BeforeClass
    public static void beforeAbstractConsoleIntegrationTest() throws Exception {
        System.setProperty(HealthChecker.ENABLED, "false");
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);

        if (REPLACE_CONFIG) initLocalConfig();
    }

    @AfterClass
    public static void afterAbstractConsoleIntegrationTest() {
        if (REPLACE_CONFIG) recoverLocalConfig();
    }

    protected static void initConfigDir() throws Exception {
        File qconfigDir = new File(CONFIG_QCONFIG_DIR_PATH);
        File dalDir = new File(CONFIG_DAL_DIR_PATH);

        if (!qconfigDir.exists() && !qconfigDir.mkdirs()) {
            throw new FileSystemException(CONFIG_QCONFIG_DIR_PATH);
        }
        if (!dalDir.exists() && !dalDir.mkdirs()) {
            throw new FileSystemException(CONFIG_DAL_DIR_PATH);
        }
    }

    protected static void initLocalConfig() throws Exception {
        initConfigDir();

        replacedConfigFiles = new ArrayList<>();
        replaceConfigContent(replacedConfigFiles, CONFIG_QCONFIG_APPLICATION_PATH, AbstractCtripTest::applicationProperties);
        replaceConfigContent(replacedConfigFiles, CONFIG_QCONFIG_DATASOURCE_PATH, AbstractCtripTest::datasourceProperties);
        replaceConfigContent(replacedConfigFiles, CONFIG_QCONFIG_DATASOURCES_PATH, AbstractCtripTest::datasourcesXml);
        replaceConfigContent(replacedConfigFiles, CONFIG_QCONFIG_SSO_CLIENT_PATH, AbstractCtripTest::ssoClientProperties);
        replaceConfigContent(replacedConfigFiles, CONFIG_QCONFIG_SSO_SERVICE_PATH, AbstractCtripTest::ssoServiceProperties);
        replaceConfigContent(replacedConfigFiles, CONFIG_DAL_DATASOURCES_PATH, AbstractCtripTest::localDatabasesProperties);
    }

    protected static void recoverLocalConfig() {
        recoverConfig(replacedConfigFiles);
    }

    protected static void replaceConfigContent(List<FileContentRegister> registers, String path, ConfContentSupplier contentSupplier) throws Exception {
        File configFile = new File(path);
        FileContentRegister register = new FileContentRegister(configFile);
        register.loadFileContent();
        registers.add(register);

        if (!configFile.exists()) configFile.createNewFile();
        IOUtil.copy(contentSupplier.get(), new FileOutputStream(configFile));
    }

    protected static void recoverConfig(List<FileContentRegister> registers) {
        if (null != registers && !registers.isEmpty()) {
            registers.forEach(FileContentRegister::restoreFileContent);
        }
    }

    protected static String applicationProperties() throws Exception {
        return "";
    }

    protected static String datasourceProperties() throws Exception {
        return prepareDatasFromFile("src/test/resources/datasource.properties");
    }

    protected static String datasourcesXml() throws Exception {
        return prepareDatasFromFile("src/test/resources/local-datasources.xml");
    }

    protected static String ssoClientProperties() throws Exception {
        return "";
    }

    protected static String ssoServiceProperties() throws Exception {
        return "";
    }

    protected static String localDatabasesProperties() throws Exception {
        return prepareDatasFromFile("src/test/resources/local-databases.properties");
    }

}
