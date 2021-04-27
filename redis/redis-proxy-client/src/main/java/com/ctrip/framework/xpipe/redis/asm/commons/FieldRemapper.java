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

package com.ctrip.framework.xpipe.redis.asm.commons;

import com.ctrip.framework.xpipe.redis.asm.AnnotationVisitor;
import com.ctrip.framework.xpipe.redis.asm.FieldVisitor;
import com.ctrip.framework.xpipe.redis.asm.Opcodes;
import com.ctrip.framework.xpipe.redis.asm.TypePath;

/**
 * A {@link FieldVisitor} that remaps types with a {@link Remapper}.
 *
 * @author Eugene Kuleshov
 */
public class FieldRemapper extends FieldVisitor {

    /**
     * The remapper used to remap the types in the visited field.
     */
    protected final Remapper remapper;

    /**
     * Constructs a new {@link FieldRemapper}. <i>Subclasses must not use this constructor</i>.
     * Instead, they must use the {@link #FieldRemapper(int, FieldVisitor, Remapper)} version.
     *
     * @param fieldVisitor the field visitor this remapper must deleted to.
     * @param remapper     the remapper to use to remap the types in the visited field.
     */
    public FieldRemapper(final FieldVisitor fieldVisitor, final Remapper remapper) {
        this(Opcodes.ASM7, fieldVisitor, remapper);
    }

    /**
     * Constructs a new {@link FieldRemapper}.
     *
     * @param api          the ASM API version supported by this remapper. Must be one of {@link
     *                     com.ctrip.framework.xpipe.redis.asm.Opcodes#ASM4}, {@link com.ctrip.framework.xpipe.redis.asm.Opcodes#ASM5} or {@link
     *                     com.ctrip.framework.xpipe.redis.asm.Opcodes#ASM6}.
     * @param fieldVisitor the field visitor this remapper must deleted to.
     * @param remapper     the remapper to use to remap the types in the visited field.
     */
    protected FieldRemapper(final int api, final FieldVisitor fieldVisitor, final Remapper remapper) {
        super(api, fieldVisitor);
        this.remapper = remapper;
    }

    @Override
    public AnnotationVisitor visitAnnotation(final String descriptor, final boolean visible) {
        AnnotationVisitor annotationVisitor =
                super.visitAnnotation(remapper.mapDesc(descriptor), visible);
        return annotationVisitor == null
                ? null
                : new AnnotationRemapper(api, annotationVisitor, remapper);
    }

    @Override
    public AnnotationVisitor visitTypeAnnotation(
            final int typeRef, final TypePath typePath, final String descriptor, final boolean visible) {
        AnnotationVisitor annotationVisitor =
                super.visitTypeAnnotation(typeRef, typePath, remapper.mapDesc(descriptor), visible);
        return annotationVisitor == null
                ? null
                : new AnnotationRemapper(api, annotationVisitor, remapper);
    }
}
