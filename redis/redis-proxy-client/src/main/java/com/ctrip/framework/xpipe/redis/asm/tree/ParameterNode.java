// ASM: a very small and fast Java bytecode manipulation framework
// Copyright (c) 2000-2011 INRIA, France Telecom
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the copyright holders nor the names of its
//    contributors may be used to endorse or promote products derived from
//    this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
package com.ctrip.framework.xpipe.redis.asm.tree;

import com.ctrip.framework.xpipe.redis.asm.MethodVisitor;

/**
 * A node that represents a parameter of a method.
 *
 * @author Remi Forax
 */
public class ParameterNode {

    /**
     * The parameter's name.
     */
    public String name;

    /**
     * The parameter's access flags (see {@link com.ctrip.framework.xpipe.redis.asm.Opcodes}). Valid values are {@code
     * ACC_FINAL}, {@code ACC_SYNTHETIC} and {@code ACC_MANDATED}.
     */
    public int access;

    /**
     * Constructs a new {@link ParameterNode}.
     *
     * @param access The parameter's access flags. Valid values are {@code ACC_FINAL}, {@code
     *               ACC_SYNTHETIC} or/and {@code ACC_MANDATED} (see {@link com.ctrip.framework.xpipe.redis.asm.Opcodes}).
     * @param name   the parameter's name.
     */
    public ParameterNode(final String name, final int access) {
        this.name = name;
        this.access = access;
    }

    /**
     * Makes the given visitor visit this parameter declaration.
     *
     * @param methodVisitor a method visitor.
     */
    public void accept(final MethodVisitor methodVisitor) {
        methodVisitor.visitParameter(name, access);
    }
}
