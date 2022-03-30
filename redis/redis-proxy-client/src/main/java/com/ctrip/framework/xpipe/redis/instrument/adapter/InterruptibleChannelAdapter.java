package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.alibaba.arthas.deps.org.objectweb.asm.ClassVisitor;
import com.alibaba.arthas.deps.org.objectweb.asm.MethodVisitor;

/**
 * @author lishanglin
 * date 2022/2/10
 */
public class InterruptibleChannelAdapter extends AbstractSocketAdapter {

    public InterruptibleChannelAdapter(ClassVisitor cv) {
        super(cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        if (!isInterface && mv != null && name.equals("close")) {
            mv = new CloseMethodAdapter(mv);
        }
        return mv;
    }

}
