package com.ctrip.framework.xpipe.redis.agent.adaptor;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class CloseMethodAdaptor extends MethodVisitor {

    public CloseMethodAdaptor(MethodVisitor mv) {
        super(Opcodes.ASM4, mv);
    }

    @Override
    public void visitCode() {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "com/ctrip/framework/xpipe/redis/utils/ConnectionUtil", "removeAddress", "(Ljava/lang/Object;)Ljava/net/SocketAddress;", false);
    }
}