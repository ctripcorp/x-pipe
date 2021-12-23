package com.ctrip.framework.xpipe.redis.instrument;

import com.ctrip.framework.xpipe.redis.utils.JarFileUrlJar;
import com.ctrip.framework.xpipe.redis.utils.Tools;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.regex.Pattern;

public class ProxyAgentTool {

    public static final String VirtualMachineClassName = "com.sun.tools.attach.VirtualMachine";

    public static final String HotspotVMName = "sun.tools.attach.HotSpotVirtualMachine";
    
    public static final String DisableLoadProxyAgentJar = "DisableLoadProxyAgentJar";

    private static Class<?> VmProviderClass = null;

    private static volatile boolean isLoaded = false;

    public static synchronized void startUp() {
        if (!isLoaded) {
            if(System.getProperty(DisableLoadProxyAgentJar, "false").equals("true")) {
                return;
            }
            loadAgent();
            isLoaded = true;
        }
    }
    
    private static URL findProxyClientJar(String dir) throws MalformedURLException {
        File file = new File(dir);
        File[] tempList = file.listFiles();
        if (null == tempList) return null;

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                if(Pattern.matches(".*/redis-proxy-client-(\\d+).(\\d+).(\\d)+.jar", tempList[i].toString())) {
                    return tempList[i].toURI().toURL();    
                }
            }
        }
        return null;
    }

    private static void loadAgent() {

        String msg = null;
        try {
            Object VM;
            Method loadAgentMethod;
            Method detachMethod;
            Class<?> vmClass = Tools.loadJDKToolClass(VirtualMachineClassName);
            Class<?> hotspotVMClass = Tools.loadJDKToolClass(HotspotVMName);
            String pid = Tools.currentPID();
            if (VmProviderClass != null) {
                Object vmProvider = VmProviderClass.newInstance();
                VM = VmProviderClass.getMethod("attachVirtualMachine", String.class).invoke(vmProvider, pid);
                loadAgentMethod = VM.getClass().getMethod("loadAgent", String.class, String.class);
                detachMethod = VM.getClass().getMethod("detach");
            } else {
                Method attacheMethod = vmClass.getMethod("attach", String.class);
                VM = attacheMethod.invoke(null, pid);
                VmProviderClass = vmClass.getMethod("provider").invoke(VM).getClass();
                loadAgentMethod = hotspotVMClass.getMethod("loadAgent", String.class, String.class);
                detachMethod = vmClass.getMethod("detach");
            }

            CodeSource src = AgentMain.class.getProtectionDomain().getCodeSource();
            URL url = src.getLocation();
            if(Pattern.matches(".*/redis-proxy-client/target/classes/", url.toString())) {
                url = findProxyClientJar(url.getPath() + "../");
                if(url == null) {
                    throw new RuntimeException("not find proxy-client.jar, please `cd redis/redis-proxy` run `mvn install`");
                }
            }
            URI uri = url.toURI();
            String protocol = uri.toString();
            String proxyFile;
            String jarPath;

            if (("jar").equals(url.getProtocol())) {
                if (protocol.endsWith("!/")) {
                    protocol = protocol.substring(0, protocol.length() - 2);
                }
                JarFileUrlJar jarFileUrlJar = new JarFileUrlJar(new URL(protocol));
                jarPath = jarFileUrlJar.getJarFilePath();
                proxyFile = jarPath;
                msg = String.format("[AgentMain] proxy jarFile:%s", jarPath);
                jarFileUrlJar.close();
            } else {
                msg = String.format("[AgentMain] proxy uri:%s", uri);
                proxyFile = uri.getSchemeSpecificPart();
                jarPath = Paths.get(uri).toString();
            }

            loadAgentMethod.invoke(VM, jarPath, proxyFile);
            detachMethod.invoke(VM);
        } catch (Exception e) {
            throw new RuntimeException(msg, e);
        }
    }

}
