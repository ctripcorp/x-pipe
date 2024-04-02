package com.ctrip.framework.xpipe.redis.instrument.adapter;


import com.alibaba.deps.org.objectweb.asm.ClassVisitor;
import com.alibaba.deps.org.objectweb.asm.Label;
import com.alibaba.deps.org.objectweb.asm.MethodVisitor;
import com.alibaba.deps.org.objectweb.asm.Opcodes;

import static com.ctrip.framework.xpipe.redis.utils.Constants.CONNECT_CLASS;

public class SocketAdapter extends AbstractSocketAdapter {

    public SocketAdapter(ClassVisitor cv) {
        super(cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        if (!isInterface && mv != null && name.equals("connect")) {
            mv = new ConnectMethodAdaptor(mv);
        } else if (!isInterface && mv != null && name.equals("close")) {
            mv = new CloseMethodAdapter(mv);
        }
        return mv;
    }

    class ConnectMethodAdaptor extends MethodVisitor {
        public ConnectMethodAdaptor(MethodVisitor mv) {
            super(Opcodes.ASM7, mv);
        }

        /**
         * public void connect(SocketAddress endpoint, int timeout) {
         *     String toString = endpoint.toString();
         *     endpoint = ConnectionUtil.getAddress(this, endpoint);
         *     if (!endpoint.toString().equals(toString)) {
         *         ConnectionUtil.connectToProxy(this, endpoint, timeout);
         *         return;
         *     }
         * }
         */
        @Override
        public void visitCode() {

            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/net/InetSocketAddress", "toString", "()Ljava/lang/String;", false);
            mv.visitVarInsn(Opcodes.ASTORE, 3);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "getAddress", "(Ljava/lang/Object;Ljava/net/InetSocketAddress;)Ljava/net/InetSocketAddress;", false);
            mv.visitVarInsn(Opcodes.ASTORE, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/net/InetSocketAddress", "toString", "()Ljava/lang/String;", false);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/String", "equals", "(Ljava/lang/Object;)Z", false);
            Label label = new Label();
            mv.visitJumpInsn(Opcodes.IFNE, label);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ILOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "connectToProxy", "(Ljava/net/Socket;Ljava/net/InetSocketAddress;I)V", false);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(label);

        }
    }
}
