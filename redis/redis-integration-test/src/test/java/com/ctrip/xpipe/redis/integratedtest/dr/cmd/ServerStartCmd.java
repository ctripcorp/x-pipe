package com.ctrip.xpipe.redis.integratedtest.dr.cmd;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author lishanglin
 * date 2021/1/21
 */
public class ServerStartCmd extends AbstractForkProcessCmd {

    private String mainClass;

    private Map<String, String> args;

    private String id;

    public ServerStartCmd(String id, String mainClass, Map<String, String> args, ExecutorService executors) {
        super(executors);
        this.id = id;
        this.mainClass = mainClass;
        this.args = args;
    }

    public String getId() {
        return id;
    }

    @Override
    protected void doExecute() throws Exception {
        String javaHome = System.getProperty("java.home");
        String javaExecutable = javaHome +
                File.separator + "bin" +
                File.separator + "java";

        URL[] urls = ((URLClassLoader) Thread.currentThread().getContextClassLoader()).getURLs();
        String classPath = ".:";
        for (URL url: urls) {
            classPath += url.getPath() + ":";
        }
        classPath = classPath.substring(0, classPath.length() - 1);

        String cmd = javaExecutable + " -classpath \"" + classPath + "\" ";
        StringBuilder argBuilder = new StringBuilder();
        args.forEach((key, value) -> {
            argBuilder.append(String.format("-D%s=%s ", key, value));
        });

        execCmd(cmd + argBuilder.toString() + mainClass);
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return "ServerStartCmd-" + id + "-" + mainClass;
    }

}
