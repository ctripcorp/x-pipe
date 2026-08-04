package com.ctrip.xpipe.redis.meta.server.tfs.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import java.io.IOException;

/**
 * Hand-written subset of {@code tfs.tfs_gateway.ForceCloseDirResponse} (proto3).
 * Fields: status=1 ({@link TfsStatus}).
 */
public final class ForceCloseDirResponse {

    private final TfsStatus status;

    public ForceCloseDirResponse(TfsStatus status) {
        this.status = status;
    }

    public TfsStatus getStatus() {
        return status;
    }

    public byte[] toByteArray() {
        try {
            byte[] statusBytes = status == null ? new byte[0] : status.toByteArray();
            int size = status == null ? 0 : CodedOutputStream.computeByteArraySize(1, statusBytes);
            byte[] result = new byte[size];
            CodedOutputStream output = CodedOutputStream.newInstance(result);
            if (status != null) {
                output.writeByteArray(1, statusBytes);
            }
            output.checkNoSpaceLeft();
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("encode ForceCloseDirResponse failed", e);
        }
    }

    public static ForceCloseDirResponse parseFrom(byte[] data) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(data);
        TfsStatus status = null;
        while (!input.isAtEnd()) {
            int tag = input.readTag();
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1:
                    ByteString bytes = input.readBytes();
                    status = TfsStatus.parseFrom(bytes.toByteArray());
                    break;
                default:
                    input.skipField(tag);
                    break;
            }
        }
        return new ForceCloseDirResponse(status);
    }
}
