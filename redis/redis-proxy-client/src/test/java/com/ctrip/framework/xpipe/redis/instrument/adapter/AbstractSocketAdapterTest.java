package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.alibaba.deps.org.objectweb.asm.ClassWriter;
import com.alibaba.deps.org.objectweb.asm.MethodVisitor;
import com.alibaba.deps.org.objectweb.asm.Opcodes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2021/4/23
 */
public abstract class AbstractSocketAdapterTest {

    protected ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    protected AbstractSocketAdapter socketAdaptor;

    protected int version = Opcodes.ASM7;

    protected int access = Opcodes.ACC_PUBLIC;

    protected String connectName = "connect";

    protected String connectDesc = "(Ljava/net/Socket;Ljava/net/InetSocketAddress;I)V";

    protected String closeName = "close";

    protected String closeDesc = "()V";

    protected String signature = "";

    protected String[] exceptions = new String[0];

    protected String[] interfaces = new String[0];


    @Test
    public void testVisitConnectMethod() {
        // visit header
        socketAdaptor.visit(version, access, connectName, connectDesc, signature, interfaces);
        // visit method
        MethodVisitor methodVisitor = socketAdaptor.visitMethod(access, connectName, connectDesc, signature, exceptions);
        assertConnect(methodVisitor);

        try {
            methodVisitor.visitCode();
        } catch (Throwable t) {
            Assert.fail();
        }
    }

    protected abstract void assertConnect(MethodVisitor methodVisitor);

    @Test
    public void testVisitCloseMethod() {
        // visit header
        socketAdaptor.visit(version, access, closeName, closeDesc, signature, interfaces);
        // visit method
        MethodVisitor methodVisitor = socketAdaptor.visitMethod(access, closeName, closeDesc, signature, exceptions);
        Assert.assertTrue(methodVisitor instanceof CloseMethodAdapter);

        try {
            CloseMethodAdapter closeMethodAdaptor = (CloseMethodAdapter) methodVisitor;
            closeMethodAdaptor.visitCode();
        } catch (Throwable t) {
            Assert.fail();
        }
    }

}
