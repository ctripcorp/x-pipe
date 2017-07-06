package com.ctrip.xpipe.tuple;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;

import java.io.IOException;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 06, 2017
 */
public class PairDeserial extends StdDeserializer<Pair> implements ContextualDeserializer {

    private JavaType javaType;
    private KeyDeserializer _keyDeserializer;
    private JsonDeserializer<Object> _valueDeserializer;
    private TypeDeserializer _valueTypeDeserializer;

    protected PairDeserial() {
        super((Class<?>) null);
    }

    public PairDeserial(JavaType type,
                        KeyDeserializer keyDeser, JsonDeserializer<Object> valueDeser,
                        TypeDeserializer valueTypeDeser) {
        super(type);
        this.javaType = type;
        this._keyDeserializer = keyDeser;
        this._valueDeserializer = valueDeser;
        this._valueTypeDeserializer = valueTypeDeser;
    }

    @Override
    public Pair<Object, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

        JsonToken t = p.getCurrentToken();
        if (t != JsonToken.START_OBJECT && t != JsonToken.FIELD_NAME && t != JsonToken.END_OBJECT) {
            // String may be ok however:
            // slightly redundant (since String was passed above), but
            return _deserializeFromEmpty(p, ctxt);
        }
        if (t == JsonToken.START_OBJECT) {
            t = p.nextToken();
        }
        if (t != JsonToken.FIELD_NAME) {
            if (t == JsonToken.END_OBJECT) {
                ctxt.reportMappingException("Can not deserialize a Map.Entry out of empty JSON Object");
                return null;
            }
            return (Pair<Object, Object>) ctxt.handleUnexpectedToken(handledType(), p);
        }

        final KeyDeserializer keyDes = _keyDeserializer;
        final JsonDeserializer<Object> valueDes = _valueDeserializer;
        final TypeDeserializer typeDeser = _valueTypeDeserializer;

        final String keyStr = p.getCurrentName();
        Object key = keyDes.deserializeKey(keyStr, ctxt);
        Object value = null;
        // And then the value...
        t = p.nextToken();
        try {
            // Note: must handle null explicitly here; value deserializers won't
            if (t == JsonToken.VALUE_NULL) {
                value = valueDes.getNullValue(ctxt);
            } else if (typeDeser == null) {
                value = valueDes.deserialize(p, ctxt);
            } else {
                value = valueDes.deserializeWithType(p, ctxt, typeDeser);
            }
        } catch (Exception e) {
            throw new IllegalStateException("pair value deserialize error", e);
        }

        // Close, but also verify that we reached the END_OBJECT
        t = p.nextToken();
        if (t != JsonToken.END_OBJECT) {
            if (t == JsonToken.FIELD_NAME) { // most likely
                ctxt.reportMappingException("Problem binding JSON into Map.Entry: more than one entry in JSON (second field: '"+p.getCurrentName()+"')");
            } else {
                // how would this occur?
                ctxt.reportMappingException("Problem binding JSON into Map.Entry: unexpected content after JSON Object entry: "+t);
            }
            return null;
        }
        return new Pair<>(key, value);
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {

        JavaType contextualType = ctxt.getContextualType();

        List<JavaType> typeParameters = contextualType.getBindings().getTypeParameters();
        if (typeParameters.size() != 2) {
            throw new IllegalStateException("size should be 2");
        }

        JavaType kt = typeParameters.get(0);
        JavaType vt = typeParameters.get(1);

        KeyDeserializer keyDeserializer = ctxt.findKeyDeserializer(kt, property);
        JsonDeserializer<Object> contextualValueDeserializer = ctxt.findContextualValueDeserializer(vt, property);
        return new PairDeserial(contextualType, keyDeserializer, contextualValueDeserializer, null);
    }
}
