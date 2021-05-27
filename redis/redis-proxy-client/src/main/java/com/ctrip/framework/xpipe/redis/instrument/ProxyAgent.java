package com.ctrip.framework.xpipe.redis.instrument;


import com.alibaba.arthas.deps.org.objectweb.asm.ClassReader;
import com.alibaba.arthas.deps.org.objectweb.asm.ClassWriter;
import com.ctrip.framework.xpipe.redis.instrument.adapter.SocketAdapter;
import com.ctrip.framework.xpipe.redis.instrument.adapter.SocketChannelImplAdapter;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import static com.ctrip.framework.xpipe.redis.utils.Constants.NIO_SOCKET;
import static com.ctrip.framework.xpipe.redis.utils.Constants.SOCKET;

public class ProxyAgent implements ClassFileTransformer {

    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        className = className.replace("/", ".");
        if (SOCKET.equals(className)) {
            ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            SocketAdapter socketAdapter = new SocketAdapter(classWriter);
            ClassReader classReader = new ClassReader(classfileBuffer);
            classReader.accept(socketAdapter, 0);
            return classWriter.toByteArray();
        } else if (NIO_SOCKET.equals(className)) {
            ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            SocketChannelImplAdapter socketChannelImplAdapter = new SocketChannelImplAdapter(classWriter);
            ClassReader classReader = new ClassReader(classfileBuffer);
            classReader.accept(socketChannelImplAdapter, 0);
            return classWriter.toByteArray();
        }
        return null;
    }

}
