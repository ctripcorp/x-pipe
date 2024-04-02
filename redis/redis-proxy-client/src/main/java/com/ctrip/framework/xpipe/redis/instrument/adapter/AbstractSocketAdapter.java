package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.alibaba.deps.org.objectweb.asm.ClassVisitor;
import com.alibaba.deps.org.objectweb.asm.Opcodes;

public abstract class AbstractSocketAdapter extends ClassVisitor {

    protected boolean isInterface;

    public AbstractSocketAdapter(ClassVisitor cv) {
        super(Opcodes.ASM7, cv);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        cv.visit(version, access, name, signature, superName, interfaces);
        isInterface = (access & Opcodes.ACC_INTERFACE) != 0;
    }

}