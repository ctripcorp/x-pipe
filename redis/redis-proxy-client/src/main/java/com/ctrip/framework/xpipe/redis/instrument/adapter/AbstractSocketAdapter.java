package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.ctrip.framework.xpipe.redis.asm.ClassVisitor;
import com.ctrip.framework.xpipe.redis.asm.Opcodes;

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