package com.ctrip.framework.xpipe.redis.instrument;

import com.ctrip.framework.xpipe.redis.utils.Tools;

import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.security.CodeSource;

public class ProxyAgentTool {

    public static final String VirtualMachineClassName = "com.sun.tools.attach.VirtualMachine";

    public static final String HotspotVMName = "sun.tools.attach.HotSpotVirtualMachine";

    private static Class<?> VmProviderClass = null;

    private static volatile boolean isLoaded = false;

    public static synchronized void startUp() throws Exception {
        if (!isLoaded) {
            loadAgent();
            isLoaded = true;
        }
    }

    private static void loadAgent() throws Exception {

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

        String jarPath = Paths.get(src.getLocation().toURI()).toString();
        String proxyFile = src.getLocation().toURI().getSchemeSpecificPart();
        loadAgentMethod.invoke(VM, jarPath, proxyFile);
        detachMethod.invoke(VM);
    }

}
