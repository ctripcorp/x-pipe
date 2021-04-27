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

import com.ctrip.framework.xpipe.redis.asm.ModuleVisitor;

import java.util.List;

/**
 * A node that represents an opened package with its name and the module that can access it.
 *
 * @author Remi Forax
 */
public class ModuleOpenNode {

    /**
     * The internal name of the opened package.
     */
    public String packaze;

    /**
     * The access flag of the opened package, valid values are among {@code ACC_SYNTHETIC} and {@code
     * ACC_MANDATED}.
     */
    public int access;

    /**
     * The fully qualified names (using dots) of the modules that can use deep reflection to the
     * classes of the open package, or {@literal null}.
     */
    public List<String> modules;

    /**
     * Constructs a new {@link ModuleOpenNode}.
     *
     * @param packaze the internal name of the opened package.
     * @param access  the access flag of the opened package, valid values are among {@code
     *                ACC_SYNTHETIC} and {@code ACC_MANDATED}.
     * @param modules the fully qualified names (using dots) of the modules that can use deep
     *                reflection to the classes of the open package, or {@literal null}.
     */
    public ModuleOpenNode(final String packaze, final int access, final List<String> modules) {
        this.packaze = packaze;
        this.access = access;
        this.modules = modules;
    }

    /**
     * Makes the given module visitor visit this opened package.
     *
     * @param moduleVisitor a module visitor.
     */
    public void accept(final ModuleVisitor moduleVisitor) {
        moduleVisitor.visitOpen(
                packaze, access, modules == null ? null : modules.toArray(new String[0]));
    }
}
