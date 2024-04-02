package com.ctrip.framework.xpipe.redis.instrument.adapter;


import com.alibaba.deps.org.objectweb.asm.ClassVisitor;
import com.alibaba.deps.org.objectweb.asm.Label;
import com.alibaba.deps.org.objectweb.asm.MethodVisitor;
import com.alibaba.deps.org.objectweb.asm.Opcodes;

import static com.ctrip.framework.xpipe.redis.utils.Constants.CONNECT_CLASS;

public class SocketChannelImplAdapter extends AbstractSocketAdapter {

    public SocketChannelImplAdapter(ClassVisitor cv) {
        super(cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        if (!isInterface && mv != null && name.equals("connect")) {
            mv = new ConnectMethodAdapter(mv);
        } else if (!isInterface && mv != null && name.equals("write")) {
            mv = new WriteMethodAdapter(mv);
        } else if (!isInterface && mv != null && name.equals("close")) {
            mv = new CloseMethodAdapter(mv);
        }
        return mv;
    }

    class WriteMethodAdapter extends MethodVisitor {

        public WriteMethodAdapter(MethodVisitor methodVisitor) {
            super(Opcodes.ASM7, methodVisitor);
        }

        /**
         * invoke ConnectionUtil.sendProtocolToProxy(SocketChannel) before first write
         */
        @Override
        public void visitCode() {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "sendProtocolToProxy", "(Ljava/nio/channels/SocketChannel;)V", false);
        }
    }

    class ConnectMethodAdapter extends MethodVisitor {
        public ConnectMethodAdapter(MethodVisitor mv) {
            super(Opcodes.ASM7, mv);
        }

        /**
         * public void connect(SocketAddress sa) {
         * 	   String toString = ConnectionUtil.getString(sa);
         *     sa = ConnectionUtil.getAddress(this, sa);
         *     if (!ConnectionUtil.getString(sa).equals(toString)) {
         *         ConnectionUtil.connectToProxy(this, sa);
         *         return;
         *     }
         * }
         */
        @Override
        public void visitCode() {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "getString", "(Ljava/net/SocketAddress;)Ljava/lang/String;", false);
            mv.visitVarInsn(Opcodes.ASTORE, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "getAddress", "(Ljava/lang/Object;Ljava/net/SocketAddress;)Ljava/net/SocketAddress;", false);
            mv.visitVarInsn(Opcodes.ASTORE, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "getString", "(Ljava/net/SocketAddress;)Ljava/lang/String;", false);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/String", "equals", "(Ljava/lang/Object;)Z", false);
            Label label = new Label();
            mv.visitJumpInsn(Opcodes.IFNE, label);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, CONNECT_CLASS, "connectToProxy", "(Ljava/nio/channels/SocketChannel;Ljava/net/SocketAddress;)Z", false);
            mv.visitInsn(Opcodes.IRETURN);
            mv.visitLabel(label);
        }
    }
}