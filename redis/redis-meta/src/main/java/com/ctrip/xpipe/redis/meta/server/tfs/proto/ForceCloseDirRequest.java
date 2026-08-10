package com.ctrip.xpipe.redis.meta.server.tfs.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import java.io.IOException;

/**
 * Hand-written subset of {@code tfs.tfs_gateway.ForceCloseDirRequest} (proto3).
 * Fields: fs_id=1, dir_path=2, pod_ip=3; trace=100 is intentionally omitted.
 */
public final class ForceCloseDirRequest {

    private final String fsId;
    private final String dirPath;
    private final String podIp;

    public ForceCloseDirRequest(String fsId, String dirPath, String podIp) {
        this.fsId = fsId == null ? "" : fsId;
        this.dirPath = dirPath == null ? "" : dirPath;
        this.podIp = podIp == null ? "" : podIp;
    }

    public String getFsId() {
        return fsId;
    }

    public String getDirPath() {
        return dirPath;
    }

    public String getPodIp() {
        return podIp;
    }

    public byte[] toByteArray() {
        try {
            int size = 0;
            if (!fsId.isEmpty()) {
                size += CodedOutputStream.computeStringSize(1, fsId);
            }
            if (!dirPath.isEmpty()) {
                size += CodedOutputStream.computeStringSize(2, dirPath);
            }
            if (!podIp.isEmpty()) {
                size += CodedOutputStream.computeStringSize(3, podIp);
            }
            byte[] result = new byte[size];
            CodedOutputStream output = CodedOutputStream.newInstance(result);
            if (!fsId.isEmpty()) {
                output.writeString(1, fsId);
            }
            if (!dirPath.isEmpty()) {
                output.writeString(2, dirPath);
            }
            if (!podIp.isEmpty()) {
                output.writeString(3, podIp);
            }
            output.checkNoSpaceLeft();
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("encode ForceCloseDirRequest failed", e);
        }
    }

    public static ForceCloseDirRequest parseFrom(byte[] data) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(data);
        String fsId = "";
        String dirPath = "";
        String podIp = "";
        while (!input.isAtEnd()) {
            int tag = input.readTag();
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1:
                    fsId = input.readStringRequireUtf8();
                    break;
                case 2:
                    dirPath = input.readStringRequireUtf8();
                    break;
                case 3:
                    podIp = input.readStringRequireUtf8();
                    break;
                default:
                    input.skipField(tag);
                    break;
            }
        }
        return new ForceCloseDirRequest(fsId, dirPath, podIp);
    }
}
