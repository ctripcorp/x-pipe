package com.ctrip.xpipe.service.kafka;

import com.ctrip.framework.ckafka.codec.env.EnvProviderRegistry;
import com.ctrip.framework.ckafka.codec.payload.PayloadCodec;
import com.ctrip.framework.ckafka.codec.payload.PayloadCodecType;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author TB
 * @date 2026/4/28 12:37
 */
public class CustomAvroPayloadCodec implements PayloadCodec {
    private static final Logger log = LoggerFactory.getLogger(CustomAvroPayloadCodec.class);
    private static final int DEFAULT_CACHED_SCHEMA_SIZE = 1000;
    private final KafkaAvroDeserializer specificDeserializer;
    private final KafkaAvroDeserializer genericDeserializer;
    private final KafkaAvroSerializer serializer;

    public CustomAvroPayloadCodec() {
        Map<String, String> configs = new HashMap();
        String schemaRegistryAddress = EnvProviderRegistry.getEnvProvider().getEnv().schemaRegistryAddress();
        configs.put("schema.registry.url", schemaRegistryAddress);
        configs.put("auto.register.schemas", "false");
        configs.put("use.latest.version", "true");
        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryAddress, 1000);
        this.serializer = new CustomKafkaAvroSerializer(registryClient);
        this.serializer.configure(configs, false);
        configs.put("specific.avro.reader", Boolean.TRUE.toString());
        this.specificDeserializer = new KafkaAvroDeserializer(new CachedSchemaRegistryClient(schemaRegistryAddress, 1000));
        this.specificDeserializer.configure(configs, false);
        configs.put("specific.avro.reader", Boolean.FALSE.toString());
        this.genericDeserializer = new KafkaAvroDeserializer(new CachedSchemaRegistryClient(schemaRegistryAddress, 1000));
        this.genericDeserializer.configure(configs, false);
    }

    public PayloadCodecType getType() {
        return PayloadCodecType.AVRO;
    }

    public <T> T decode(byte[] source, Class<T> clazz) {
        return (T)(clazz == GenericRecord.class ? this.genericDeserializer : this.specificDeserializer).deserialize((String)null, source);
    }

    public byte[] encode(String topic, Object obj) {
        try {
            return this.serializer.serialize(topic, obj);
        } catch (Exception e) {
            log.error("Serialize avro object failed!", e);
            throw e;
        }
    }
}

