package com.ctrip.framework.xpipe.redis.agent.adaptor;

import org.objectweb.asm.*;

public abstract class AbstractSocketAdaptor extends ClassVisitor {

    protected boolean isInterface;

    public AbstractSocketAdaptor(ClassVisitor cv) {
        super(Opcodes.ASM7_EXPERIMENTAL, cv);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        cv.visit(version, access, name, signature, superName, interfaces);
        isInterface = (access & Opcodes.ACC_INTERFACE) != 0;
    }

}