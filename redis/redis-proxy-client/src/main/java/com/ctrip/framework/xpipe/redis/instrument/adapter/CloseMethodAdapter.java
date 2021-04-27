package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.ctrip.framework.xpipe.redis.asm.MethodVisitor;
import com.ctrip.framework.xpipe.redis.asm.Opcodes;

public class CloseMethodAdapter extends MethodVisitor {

    public CloseMethodAdapter(MethodVisitor mv) {
        super(Opcodes.ASM7, mv);
    }

    @Override
    public void visitCode() {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "com/ctrip/framework/xpipe/redis/utils/ConnectionUtil", "removeAddress", "(Ljava/lang/Object;)Ljava/net/SocketAddress;", false);
    }
}