package com.ctrip.framework.xpipe.redis.instrument.adapter;


import com.alibaba.deps.org.objectweb.asm.MethodVisitor;
import com.alibaba.deps.org.objectweb.asm.Opcodes;

import static com.ctrip.framework.xpipe.redis.utils.Constants.CONNECT_CLASS;

public class CloseMethodAdapter extends MethodVisitor {

    public CloseMethodAdapter(MethodVisitor mv) {
        super(Opcodes.ASM7, mv);
    }

    @Override
    public void visitCode() {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "removeAddress", "(Ljava/lang/Object;)Ljava/net/SocketAddress;", false);
    }
}