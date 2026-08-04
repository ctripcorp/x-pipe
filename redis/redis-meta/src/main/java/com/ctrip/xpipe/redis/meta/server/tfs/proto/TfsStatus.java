package com.ctrip.xpipe.redis.meta.server.tfs.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import java.io.IOException;

/**
 * Hand-written subset of {@code common.pb.Status} (proto3).
 */
public final class TfsStatus {

    private final int errorCode;
    private final String message;

    public TfsStatus(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message == null ? "" : message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getMessage() {
        return message;
    }

    public byte[] toByteArray() {
        try {
            int size = 0;
            size += CodedOutputStream.computeInt32Size(1, errorCode);
            if (!message.isEmpty()) {
                size += CodedOutputStream.computeStringSize(2, message);
            }
            byte[] result = new byte[size];
            CodedOutputStream output = CodedOutputStream.newInstance(result);
            output.writeInt32(1, errorCode);
            if (!message.isEmpty()) {
                output.writeString(2, message);
            }
            output.checkNoSpaceLeft();
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("encode Status failed", e);
        }
    }

    public static TfsStatus parseFrom(byte[] data) throws IOException {
        return parseFrom(CodedInputStream.newInstance(data));
    }

    static TfsStatus parseFrom(CodedInputStream input) throws IOException {
        int errorCode = 0;
        String message = "";
        while (!input.isAtEnd()) {
            int tag = input.readTag();
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1:
                    errorCode = input.readInt32();
                    break;
                case 2:
                    message = input.readStringRequireUtf8();
                    break;
                default:
                    input.skipField(tag);
                    break;
            }
        }
        return new TfsStatus(errorCode, message);
    }
}
