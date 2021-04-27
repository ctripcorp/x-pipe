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
package com.ctrip.framework.xpipe.redis.asm.util;

import com.ctrip.framework.xpipe.redis.asm.Opcodes;
import com.ctrip.framework.xpipe.redis.asm.signature.SignatureVisitor;

/**
 * A {@link SignatureVisitor} that builds the Java generic type declaration corresponding to the
 * signature it visits.
 *
 * @author Eugene Kuleshov
 * @author Eric Bruneton
 */
public final class TraceSignatureVisitor extends SignatureVisitor {

    private static final String COMMA_SEPARATOR = ", ";
    private static final String EXTENDS_SEPARATOR = " extends ";
    private static final String IMPLEMENTS_SEPARATOR = " implements ";

    /**
     * Whether the visited signature is a class signature of a Java interface.
     */
    private final boolean isInterface;

    /**
     * The Java generic type declaration corresponding to the visited signature.
     */
    private final StringBuilder declaration;

    /**
     * The Java generic method return type declaration corresponding to the visited signature.
     */
    private StringBuilder returnType;

    /**
     * The Java generic exception types declaration corresponding to the visited signature.
     */
    private StringBuilder exceptions;

    /**
     * Whether {@link #visitFormalTypeParameter} has been called.
     */
    private boolean formalTypeParameterVisited;

    /**
     * Whether {@link #visitInterfaceBound} has been called.
     */
    private boolean interfaceBoundVisited;

    /**
     * Whether {@link #visitParameterType} has been called.
     */
    private boolean parameterTypeVisited;

    /**
     * Whether {@link #visitInterface} has been called.
     */
    private boolean interfaceVisited;

    /**
     * The stack used to keep track of class types that have arguments. Each element of this stack is
     * a boolean encoded in one bit. The top of the stack is the least significant bit. Pushing false
     * = *2, pushing true = *2+1, popping = /2.
     */
    private int argumentStack;

    /**
     * The stack used to keep track of array class types. Each element of this stack is a boolean
     * encoded in one bit. The top of the stack is the lowest order bit. Pushing false = *2, pushing
     * true = *2+1, popping = /2.
     */
    private int arrayStack;

    /**
     * The separator to append before the next visited class or inner class type.
     */
    private String separator = "";

    /**
     * Constructs a new {@link TraceSignatureVisitor}.
     *
     * @param accessFlags for class type signatures, the access flags of the class.
     */
    public TraceSignatureVisitor(final int accessFlags) {
        super(Opcodes.ASM7);
        this.isInterface = (accessFlags & Opcodes.ACC_INTERFACE) != 0;
        this.declaration = new StringBuilder();
    }

    private TraceSignatureVisitor(final StringBuilder stringBuilder) {
        super(Opcodes.ASM7);
        this.isInterface = false;
        this.declaration = stringBuilder;
    }

    @Override
    public void visitFormalTypeParameter(final String name) {
        declaration.append(formalTypeParameterVisited ? COMMA_SEPARATOR : "<").append(name);
        formalTypeParameterVisited = true;
        interfaceBoundVisited = false;
    }

    @Override
    public SignatureVisitor visitClassBound() {
        separator = EXTENDS_SEPARATOR;
        startType();
        return this;
    }

    @Override
    public SignatureVisitor visitInterfaceBound() {
        separator = interfaceBoundVisited ? COMMA_SEPARATOR : EXTENDS_SEPARATOR;
        interfaceBoundVisited = true;
        startType();
        return this;
    }

    @Override
    public SignatureVisitor visitSuperclass() {
        endFormals();
        separator = EXTENDS_SEPARATOR;
        startType();
        return this;
    }

    @Override
    public SignatureVisitor visitInterface() {
        if (interfaceVisited) {
            separator = COMMA_SEPARATOR;
        } else {
            separator = isInterface ? EXTENDS_SEPARATOR : IMPLEMENTS_SEPARATOR;
            interfaceVisited = true;
        }
        startType();
        return this;
    }

    @Override
    public SignatureVisitor visitParameterType() {
        endFormals();
        if (parameterTypeVisited) {
            declaration.append(COMMA_SEPARATOR);
        } else {
            declaration.append('(');
            parameterTypeVisited = true;
        }
        startType();
        return this;
    }

    @Override
    public SignatureVisitor visitReturnType() {
        endFormals();
        if (parameterTypeVisited) {
            parameterTypeVisited = false;
        } else {
            declaration.append('(');
        }
        declaration.append(')');
        returnType = new StringBuilder();
        return new TraceSignatureVisitor(returnType);
    }

    @Override
    public SignatureVisitor visitExceptionType() {
        if (exceptions == null) {
            exceptions = new StringBuilder();
        } else {
            exceptions.append(COMMA_SEPARATOR);
        }
        return new TraceSignatureVisitor(exceptions);
    }

    @Override
    public void visitBaseType(final char descriptor) {
        switch (descriptor) {
            case 'V':
                declaration.append("void");
                break;
            case 'B':
                declaration.append("byte");
                break;
            case 'J':
                declaration.append("long");
                break;
            case 'Z':
                declaration.append("boolean");
                break;
            case 'I':
                declaration.append("int");
                break;
            case 'S':
                declaration.append("short");
                break;
            case 'C':
                declaration.append("char");
                break;
            case 'F':
                declaration.append("float");
                break;
            case 'D':
                declaration.append("double");
                break;
            default:
                throw new IllegalArgumentException();
        }
        endType();
    }

    @Override
    public void visitTypeVariable(final String name) {
        declaration.append(separator).append(name);
        separator = "";
        endType();
    }

    @Override
    public SignatureVisitor visitArrayType() {
        startType();
        arrayStack |= 1;
        return this;
    }

    @Override
    public void visitClassType(final String name) {
        if ("java/lang/Object".equals(name)) {
            // 'Map<java.lang.Object,java.util.List>' or 'abstract public V get(Object key);' should have
            // Object 'but java.lang.String extends java.lang.Object' is unnecessary.
            boolean needObjectClass = argumentStack % 2 != 0 || parameterTypeVisited;
            if (needObjectClass) {
                declaration.append(separator).append(name.replace('/', '.'));
            }
        } else {
            declaration.append(separator).append(name.replace('/', '.'));
        }
        separator = "";
        argumentStack *= 2;
    }

    @Override
    public void visitInnerClassType(final String name) {
        if (argumentStack % 2 != 0) {
            declaration.append('>');
        }
        argumentStack /= 2;
        declaration.append('.');
        declaration.append(separator).append(name.replace('/', '.'));
        separator = "";
        argumentStack *= 2;
    }

    @Override
    public void visitTypeArgument() {
        if (argumentStack % 2 == 0) {
            ++argumentStack;
            declaration.append('<');
        } else {
            declaration.append(COMMA_SEPARATOR);
        }
        declaration.append('?');
    }

    @Override
    public SignatureVisitor visitTypeArgument(final char tag) {
        if (argumentStack % 2 == 0) {
            ++argumentStack;
            declaration.append('<');
        } else {
            declaration.append(COMMA_SEPARATOR);
        }

        if (tag == EXTENDS) {
            declaration.append("? extends ");
        } else if (tag == SUPER) {
            declaration.append("? super ");
        }

        startType();
        return this;
    }

    @Override
    public void visitEnd() {
        if (argumentStack % 2 != 0) {
            declaration.append('>');
        }
        argumentStack /= 2;
        endType();
    }

    // -----------------------------------------------------------------------------------------------

    /**
     * Returns the Java generic type declaration corresponding to the visited signature.
     *
     * @return the Java generic type declaration corresponding to the visited signature.
     */
    public String getDeclaration() {
        return declaration.toString();
    }

    /**
     * Returns the Java generic method return type declaration corresponding to the visited signature.
     *
     * @return the Java generic method return type declaration corresponding to the visited signature.
     */
    public String getReturnType() {
        return returnType == null ? null : returnType.toString();
    }

    /**
     * Returns the Java generic exception types declaration corresponding to the visited signature.
     *
     * @return the Java generic exception types declaration corresponding to the visited signature.
     */
    public String getExceptions() {
        return exceptions == null ? null : exceptions.toString();
    }

    // -----------------------------------------------------------------------------------------------

    private void endFormals() {
        if (formalTypeParameterVisited) {
            declaration.append('>');
            formalTypeParameterVisited = false;
        }
    }

    private void startType() {
        arrayStack *= 2;
    }

    private void endType() {
        if (arrayStack % 2 == 0) {
            arrayStack /= 2;
        } else {
            while (arrayStack % 2 != 0) {
                arrayStack /= 2;
                declaration.append("[]");
            }
        }
    }
}
