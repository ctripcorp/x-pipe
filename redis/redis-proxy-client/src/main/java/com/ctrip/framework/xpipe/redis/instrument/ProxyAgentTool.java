package com.ctrip.framework.xpipe.redis.instrument;

import com.ctrip.framework.xpipe.redis.utils.Tools;

import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.security.CodeSource;

public class ProxyAgentTool {

    public static final String virtualMachineClassName = "com.sun.tools.attach.VirtualMachine";

    public static final String hotspotVMName = "sun.tools.attach.HotSpotVirtualMachine";

    protected static Class<?> vmProviderClass = null;

    private static Boolean isLoaded = false;

    public static synchronized void startUp() {
        if (!isLoaded) {
            try {
                loadAgent();
                isLoaded = true;
            } catch (Throwable e) {
            }
        }
    }

    private static void loadAgent() throws Exception {

        Object VM;
        Method loadAgentMethod;
        Method detachMethod;
        Class<?> vmClass = Tools.loadJDKToolClass(virtualMachineClassName);
        Class<?> hotspotVMClass = Tools.loadJDKToolClass(hotspotVMName);
        String pid = Tools.currentPID();
        if (vmProviderClass != null) {
            Object vmProvider = vmProviderClass.newInstance();
            VM = vmProviderClass.getMethod("attachVirtualMachine", String.class).invoke(vmProvider, pid);
            loadAgentMethod = VM.getClass().getMethod("loadAgent", String.class, String.class);
            detachMethod = VM.getClass().getMethod("detach");
        } else {
            Method attacheMethod = vmClass.getMethod("attach", String.class);
            VM = attacheMethod.invoke(null, pid);
            vmProviderClass = vmClass.getMethod("provider").invoke(VM).getClass();
            loadAgentMethod = hotspotVMClass.getMethod("loadAgent", String.class, String.class);
            detachMethod = vmClass.getMethod("detach");
        }
        CodeSource src = AgentMain.class.getProtectionDomain().getCodeSource();

        String jarPath = Paths.get(src.getLocation().toURI()).toString();
        String proxyFile = src.getLocation().toURI().getSchemeSpecificPart();
        loadAgentMethod.invoke(VM, jarPath, proxyFile);
        detachMethod.invoke(VM);
    }

}
