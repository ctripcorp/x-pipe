package com.ctrip.framework.xpipe.redis.instrument;


import com.ctrip.framework.xpipe.redis.asm.ClassReader;
import com.ctrip.framework.xpipe.redis.asm.ClassWriter;
import com.ctrip.framework.xpipe.redis.instrument.adapter.SocketAdapter;
import com.ctrip.framework.xpipe.redis.instrument.adapter.SocketChannelImplAdapter;

import java.io.File;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.ProtectionDomain;

public class ProxyAgent implements ClassFileTransformer {

    private static final String ENHANCED_CLASS_DIR = "/tmp/xpipe/enhanced/";

    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        className = className.replace("/", ".");
        if (className.equals("java.net.Socket")) {
            ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            SocketAdapter socketAdapter = new SocketAdapter(classWriter);
            ClassReader classReader = new ClassReader(classfileBuffer);
            classReader.accept(socketAdapter, 0);
            return classWriter.toByteArray();
        } else if (className.equals("sun.nio.ch.SocketChannelImpl")) {
            ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            SocketChannelImplAdapter socketChannelImplAdapter = new SocketChannelImplAdapter(classWriter);
            ClassReader classReader = new ClassReader(classfileBuffer);
            classReader.accept(socketChannelImplAdapter, 0);
            byte[] res = classWriter.toByteArray();
            writeByte2File(Paths.get(ENHANCED_CLASS_DIR + className + ".class"), res);
            return res;
        }
        return null;
    }

    public static void writeByte2File(Path path, byte[] targetBytes) {
        try {
            File file = path.toFile();
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            Files.write(path, targetBytes);
        } catch (Throwable t) {

        }
    }
}
