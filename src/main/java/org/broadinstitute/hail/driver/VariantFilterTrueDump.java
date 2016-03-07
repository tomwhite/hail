package org.broadinstitute.hail.driver;

import java.util.*;

import org.objectweb.asm.*;

public class VariantFilterTrueDump implements Opcodes {

    public static byte[] dump() throws Exception {

        ClassWriter cw = new ClassWriter(0);
        FieldVisitor fv;
        MethodVisitor mv;
        AnnotationVisitor av0;

        cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, "org/broadinstitute/hail/driver/VariantFilterTrue", null, "org/broadinstitute/hail/driver/VariantFilter", null);

        {
            av0 = cw.visitAnnotation("Lscala/reflect/ScalaSignature;", true);
            av0.visit("bytes", "\u0006\u000152A!\u0001\u0002\u0001\u0017\u0009\u0009b+\u0019:jC:$h)\u001b7uKJ$&/^3\u000b\u0005\r!\u0011A\u00023sSZ,'O\u0003\u0002\u0006\r\u0005!\u0001.Y5m\u0015\u00099\u0001\"\u0001\u0008ce>\u000cG-\u001b8ti&$X\u000f^3\u000b\u0003%\u00091a\u001c:h\u0007\u0001\u0019\"\u0001\u0001\u0007\u0011\u00055qQ\"\u0001\u0002\n\u0005=\u0011!!\u0004,be&\u000cg\u000e\u001e$jYR,'\u000fC\u0003\u0012\u0001\u0011\u0005!#\u0001\u0004=S:LGO\u0010\u000b\u0002'A\u0011Q\u0002\u0001\u0005\u0006+\u0001!\u0009AF\u0001\u0007M&dG/\u001a:\u0015\u0007]iR\u0005\u0005\u0002\u001975\u0009\u0011DC\u0001\u001b\u0003\u0015\u00198-\u00197b\u0013\u0009a\u0012DA\u0004C_>dW-\u00198\u0009\u000by!\u0002\u0019A\u0010\u0002\u0003Y\u0004\"\u0001I\u0012\u000e\u0003\u0005R!A\u0009\u0003\u0002\u000fY\u000c'/[1oi&\u0011A%\u0009\u0002\u0008-\u0006\u0014\u0018.\u00198u\u0011\u00151C\u00031\u0001(\u0003\u00091\u0018\r\u0005\u0002)W5\u0009\u0011F\u0003\u0002+\u0009\u0005Y\u0011M\u001c8pi\u0006$\u0018n\u001c8t\u0013\u0009a\u0013FA\u0006B]:|G/\u0019;j_:\u001c\u0008");
            av0.visitEnd();
        }
// ATTRIBUTE ScalaSig
        {
            mv = cw.visitMethod(ACC_PUBLIC, "filter", "(Lorg/broadinstitute/hail/variant/Variant;Lorg/broadinstitute/hail/annotations/Annotations;)Z", null, null);
            mv.visitCode();
            mv.visitInsn(ICONST_1);
            mv.visitInsn(IRETURN);
            mv.visitMaxs(1, 3);
            mv.visitEnd();
        }
        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, "org/broadinstitute/hail/driver/VariantFilter", "<init>", "()V"); // , false
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        cw.visitEnd();

        return cw.toByteArray();
    }
}
