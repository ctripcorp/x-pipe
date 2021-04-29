package com.ctrip.framework.xpipe.redis.instrument;

import com.ctrip.framework.xpipe.redis.utils.JarFileUrlJar;
import com.ctrip.framework.xpipe.redis.utils.Tools;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.security.CodeSource;

public class ProxyAgentTool {

    public static final String VirtualMachineClassName = "com.sun.tools.attach.VirtualMachine";

    public static final String HotspotVMName = "sun.tools.attach.HotSpotVirtualMachine";

    private static Class<?> VmProviderClass = null;

    private static volatile boolean isLoaded = false;

    public static synchronized void startUp() {
        if (!isLoaded) {
            loadAgent();
            isLoaded = true;
        }
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
