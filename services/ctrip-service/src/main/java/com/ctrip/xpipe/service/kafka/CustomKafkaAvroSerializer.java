package com.ctrip.xpipe.service.kafka;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rules.RulePhase;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.NonRecordContainer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.serializers.schema.id.SchemaIdSerializer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * @author TB
 * @date 2026/4/29 11:42
 */
public class CustomKafkaAvroSerializer extends KafkaAvroSerializer {
    private final Map<Integer, DatumWriter<Object>> datumWriterCache = new ConcurrentHashMap<>(4);
    private final Map<String,ExtendedSchema> extendedSchemaCache = new ConcurrentHashMap<>(4);
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private ThreadLocal<ByteArrayOutputStream> baosTL = ThreadLocal.withInitial(() -> new ByteArrayOutputStream(8192));
    public CustomKafkaAvroSerializer(SchemaRegistryClient client) {
        super(client);
    }

    public byte[] serializeImpl(String subject, String topic, Headers headers, Object object, AvroSchema schema) throws SerializationException, InvalidConfigurationException {
        if (this.schemaRegistry == null) {
            StringBuilder userFriendlyMsgBuilder = new StringBuilder();
            userFriendlyMsgBuilder.append("You must configure() before serialize()");
            userFriendlyMsgBuilder.append(" or use serializer constructor with SchemaRegistryClient");
            throw new InvalidConfigurationException(userFriendlyMsgBuilder.toString());
        } else if (object == null) {
            return null;
        } else {
            String restClientErrorMsg = "";

            byte[] var13;
            try {
                SchemaId schemaId;
                if (this.autoRegisterSchema) {
                    restClientErrorMsg = "Error registering Avro schema";
                    io.confluent.kafka.schemaregistry.client.rest.entities.Schema s = this.registerWithResponse(subject, schema, this.normalizeSchema, this.propagateSchemaTags);
                    if (s.getSchema() != null) {
                        Optional<ParsedSchema> optSchema = this.schemaRegistry.parseSchema(s);
                        if (optSchema.isPresent()) {
                            schema = (AvroSchema)optSchema.get();
                            schema = schema.copy(s.getVersion());
                        }
                    }

                    schemaId = new SchemaId("AVRO", s.getId(), s.getGuid());
                } else if (this.useSchemaId >= 0) {
                    restClientErrorMsg = "Error retrieving schema ID";
                    schema = (AvroSchema)this.lookupSchemaBySubjectAndId(subject, this.useSchemaId, schema, this.idCompatStrict);
                    new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, (Integer)null, this.useSchemaId, schema);
                    schemaId = new SchemaId("AVRO", this.useSchemaId, (String)null);
                } else if (this.metadata != null) {
                    restClientErrorMsg = "Error retrieving latest with metadata '" + String.valueOf(this.metadata) + "'";
                    AbstractKafkaSchemaSerDe.ExtendedSchema extendedSchema = this.getLatestWithMetadata(subject);
                    schema = (AvroSchema)extendedSchema.getSchema();
                    schemaId = new SchemaId("AVRO", extendedSchema.getId(), extendedSchema.getGuid());
                } else if (this.useLatestVersion) {
                    restClientErrorMsg = "Error retrieving latest version of Avro schema";
                    AbstractKafkaSchemaSerDe.ExtendedSchema extendedSchema = this.extendedSchemaCache.get(subject);
                    if(extendedSchema == null) {
                        extendedSchema = this.lookupLatestVersion(subject, schema, this.latestCompatStrict);
                        extendedSchemaCache.put(subject, extendedSchema);
                    }
                    schema = (AvroSchema)extendedSchema.getSchema();
                    schemaId = new SchemaId("AVRO", extendedSchema.getId(), extendedSchema.getGuid());
                } else {
                    restClientErrorMsg = "Error retrieving Avro schema";
                    RegisterSchemaResponse response = this.schemaRegistry.getIdWithResponse(subject, schema, this.normalizeSchema);
                    schemaId = new SchemaId("AVRO", response.getId(), response.getGuid());
                }

                if (schema.ruleSet() != null && !schema.ruleSet().getDomainRules().isEmpty()) {
                    AvroSchemaUtils.setThreadLocalData(schema.rawSchema(), this.avroUseLogicalTypeConverters, this.avroReflectionAllowNull);

                    try {
                        object = this.executeRules(subject, topic, headers, RuleMode.WRITE, (ParsedSchema)null, schema, object);
                    } finally {
                        AvroSchemaUtils.clearThreadLocalData();
                    }
                }

                SchemaIdSerializer schemaIdSerializer = this.schemaIdSerializer(this.isKey);

                try {
                    ByteArrayOutputStream baos = baosTL.get();
                    baos.reset();

                    try {
                        Object value = object instanceof NonRecordContainer ? ((NonRecordContainer)object).getValue() : object;
                        Schema rawSchema = schema.rawSchema();
                        if (rawSchema.getType().equals(Schema.Type.BYTES)) {
                            if (value instanceof byte[]) {
                                baos.write((byte[])value);
                            } else {
                                if (!(value instanceof ByteBuffer)) {
                                    throw new SerializationException("Unrecognized bytes object of type: " + value.getClass().getName());
                                }

                                baos.write(((ByteBuffer)value).array());
                            }
                        } else {
                            this.writeDatum(baos, value, rawSchema,schemaId);
                        }

                        byte[] payload = baos.toByteArray();
                        payload = (byte[])this.executeRules(subject, topic, headers, payload, RulePhase.ENCODING, RuleMode.WRITE, (ParsedSchema)null, schema, payload);
                        var13 = schemaIdSerializer.serialize(topic, this.isKey, headers, payload, schemaId);
                    } catch (Throwable var37) {
                        try {
                            baos.close();
                        } catch (Throwable var36) {
                            var37.addSuppressed(var36);
                        }

                        throw var37;
                    }

                    baos.close();
                } catch (Throwable var39) {
                    if (schemaIdSerializer != null) {
                        try {
                            schemaIdSerializer.close();
                        } catch (Throwable var35) {
                            var39.addSuppressed(var35);
                        }
                    }

                    throw var39;
                }

                if (schemaIdSerializer != null) {
                    schemaIdSerializer.close();
                }
            } catch (ExecutionException ex) {
                throw new SerializationException("Error serializing Avro message", ex.getCause());
            } catch (InterruptedIOException e) {
                throw new TimeoutException("Error serializing Avro message", e);
            } catch (RuntimeException | IOException e) {
                throw new SerializationException("Error serializing Avro message", e);
            } catch (RestClientException e) {
                throw toKafkaException(e, restClientErrorMsg + String.valueOf(schema));
            } finally {
                this.postOp(object);
            }

            return var13;
        }
    }

    private void writeDatum(ByteArrayOutputStream out, Object value, Schema rawSchema,SchemaId schemaId) throws ExecutionException, IOException {
        BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out,null);
        DatumWriter<Object> writer = this.datumWriterCache.get(schemaId.getId());
        if(writer == null) {
            writer = (DatumWriter<Object>) this.getDatumWriter(value, rawSchema, this.avroUseLogicalTypeConverters, this.avroReflectionAllowNull);
            this.datumWriterCache.put(schemaId.getId(), writer);
        }
        writer.write(value, encoder);
        encoder.flush();
    }


}
