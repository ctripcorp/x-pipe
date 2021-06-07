package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

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

        URL[] urls = urlsFromClassLoader(Thread.currentThread().getContextClassLoader());
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

    private static URL[] urlsFromClassLoader(ClassLoader classLoader) {
        if (classLoader instanceof URLClassLoader) {
            return ((URLClassLoader) classLoader).getURLs();
        }
        return Stream
                .of(ManagementFactory.getRuntimeMXBean().getClassPath()
                        .split(File.pathSeparator))
                .map(ServerStartCmd::toURL).toArray(URL[]::new);
    }

    private static URL toURL(String classPathEntry) {
        try {
            return new File(classPathEntry).toURI().toURL();
        }
        catch (MalformedURLException ex) {
            throw new IllegalArgumentException(
                    "URL could not be created from '" + classPathEntry + "'", ex);
        }
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return "ServerStartCmd-" + id + "-" + mainClass;
    }

}
