package com.ctrip.xpipe.api.codec;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class RawByteArraySerializer extends StdSerializer<byte[]> {

    public RawByteArraySerializer() {
        super(byte[].class);
    }

    @Override
    public void serialize(byte[] value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        gen.writeStartArray();
        for (byte b : value) {
            gen.writeNumber(b & 0xFF);
        }
        gen.writeEndArray();
    }
}