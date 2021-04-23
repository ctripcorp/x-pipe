package com.ctrip.framework.xpipe.redis.agent;


import com.ctrip.framework.xpipe.redis.agent.adaptor.SocketAdaptor;
import com.ctrip.framework.xpipe.redis.agent.adaptor.SocketChannelImplAdaptor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

public class ProxyAgent implements ClassFileTransformer {

    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        className = className.replace("/", ".");
        if (className.equals("java.net.Socket")) {
            ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            SocketAdaptor socketAdaptor = new SocketAdaptor(classWriter);
            ClassReader classReader = new ClassReader(classfileBuffer);
            classReader.accept(socketAdaptor, 0);
            return classWriter.toByteArray();
        } else if (className.equals("sun.nio.ch.SocketChannelImpl")) {
            ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            SocketChannelImplAdaptor socketChannelImplAdaptor = new SocketChannelImplAdaptor(classWriter);
            ClassReader classReader = new ClassReader(classfileBuffer);
            classReader.accept(socketChannelImplAdaptor, 0);
            return classWriter.toByteArray();
        }
        return null;
    }
}
