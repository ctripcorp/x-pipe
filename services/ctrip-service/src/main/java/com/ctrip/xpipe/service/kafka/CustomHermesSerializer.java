package com.ctrip.xpipe.service.kafka;

import com.ctrip.framework.ckafka.codec.entity.HermesCodecException;
import com.ctrip.framework.ckafka.codec.entity.ProducerMessage;
import com.ctrip.framework.ckafka.codec.entity.PropertiesHolder;
import com.ctrip.framework.ckafka.codec.payload.PayloadCodec;
import com.ctrip.framework.ckafka.codec.util.*;
import com.ctrip.framework.foundation.Foundation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.serialization.Serializer;
import org.unidal.helper.Inets;

import java.io.IOException;
import java.util.Map;

/**
 * @author TB
 * @date 2026/4/28 11:41
 */
public class CustomHermesSerializer<T> implements Serializer<T> {
    private static final byte VERSION_V1 = 1;
    public static final String PRODUCER_IP = "ProducerIp";
    public static final String APP_ID = "AppId";
    String hermesCodecType;
    private volatile String producerIp;
    private final Object lock = new Object();
    private static PayloadCodec payloadCodec = new CustomAvroPayloadCodec();
    private String codecType;

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.hermesCodecType = (String)configs.get("hermes.codecType");
        try {
            this.codecType = CodecTypeUtil.getCodecType("bbz.fx.xpipe.ck.gtid");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] serialize(String topic, T data) {
        try {
            ProducerMessage message = new ProducerMessage(topic, data);
            byte[] body = payloadCodec.encode(topic, data);
            ByteBuf byteBuf = Unpooled.buffer(body.length + 150);
            MagicUtil.writeMagic(byteBuf);
            HermesPrimitiveCodec codec = new HermesPrimitiveCodec(byteBuf);
            byteBuf.writeByte(1);
            int indexBeginning = byteBuf.writerIndex();
            codec.writeInt(-1);
            int indexAfterWholeLen = byteBuf.writerIndex();
            codec.writeInt(-1);
            codec.writeInt(-1);
            int indexBeforeHeader = byteBuf.writerIndex();
            codec.writeString(message.getKey());
            codec.writeLong(message.getBornTime());
            codec.writeInt(0);
            codec.writeString(this.codecType);
            if (this.producerIp == null) {
                synchronized(this.lock) {
                    if (this.producerIp == null) {
                        this.producerIp = Inets.IP4.getLocalHostAddress();
                    }
                }
            }

            PropertiesHolder propertiesHolderSource = new PropertiesHolder();
            propertiesHolderSource.addDurableSysProperty("ProducerIp", this.producerIp);
            propertiesHolderSource.addDurableSysProperty("AppId", Foundation.app().getAppId());
            message.setPropertiesHolder(propertiesHolderSource);
            PropertiesHolder propertiesHolder = message.getPropertiesHolder();
            this.writeProperties(propertiesHolder.getDurableProperties(), byteBuf, codec);
            this.writeProperties(propertiesHolder.getVolatileProperties(), byteBuf, codec);
            int headerLen = byteBuf.writerIndex() - indexBeforeHeader;
            int indexBeforeBody = byteBuf.writerIndex();
            byteBuf.writeBytes(body);
            int bodyLen = byteBuf.writerIndex() - indexBeforeBody;
            codec.writeLong(ChecksumUtil.crc32(byteBuf.slice(indexBeforeHeader, headerLen + bodyLen)));
            int indexEnd = byteBuf.writerIndex();
            int wholeLen = indexEnd - indexAfterWholeLen;
            byteBuf.writerIndex(indexBeginning);
            codec.writeInt(wholeLen);
            codec.writeInt(headerLen);
            codec.writeInt(bodyLen);
            byteBuf.writerIndex(indexEnd);
            byte[] res = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(res);
            return res;
        } catch (Exception e) {
            throw new HermesCodecException("Error serializing hermes message", e);
        }
    }

    public void close() {
    }

    private void writeProperties(Map<String, String> properties, ByteBuf byteBuf, HermesPrimitiveCodec codec) {
        int writeIndexBeforeLength = byteBuf.writerIndex();
        codec.writeInt(-1);
        int writeIndexBeforeMap = byteBuf.writerIndex();
        codec.writeStringStringMap(properties);
        int mapLength = byteBuf.writerIndex() - writeIndexBeforeMap;
        int writeIndexEnd = byteBuf.writerIndex();
        byteBuf.writerIndex(writeIndexBeforeLength);
        codec.writeInt(mapLength);
        byteBuf.writerIndex(writeIndexEnd);
    }
}

