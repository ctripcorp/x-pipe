package com.ctrip.xpipe.api.kafka;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

import java.util.ArrayList;
import java.util.List;

/**
 * @author TB
 * @date 2026/4/26 23:20
 */

@org.apache.avro.specific.AvroGenerated
public class GtidKeyItem extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = -2366625245222956617L;


    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GtidKeyItem\",\"namespace\":\"com.ctrip.xpipe.api.kafka\",\"fields\":[{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"cmd\",\"type\":\"string\"},{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"seq\",\"type\":\"string\"},{\"name\":\"key\",\"type\":{\"type\":\"array\",\"items\":[\"null\",\"int\"]},\"default\":[]},{\"name\":\"subkey\",\"type\":{\"type\":\"array\",\"items\":[\"null\",\"int\"]},\"default\":[]},{\"name\":\"dbid\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"shardid\",\"type\":\"int\"}]}");
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

    private static final SpecificData MODEL$ = new SpecificData();
    static {
        MODEL$.setCustomCoders(true);
    }

    private static final BinaryMessageEncoder<GtidKeyItem> ENCODER =
            new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

    private static final BinaryMessageDecoder<GtidKeyItem> DECODER =
            new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

    /**
     * Return the BinaryMessageEncoder instance used by this class.
     * @return the message encoder used by this class
     */
    public static BinaryMessageEncoder<GtidKeyItem> getEncoder() {
        return ENCODER;
    }

    /**
     * Return the BinaryMessageDecoder instance used by this class.
     * @return the message decoder used by this class
     */
    public static BinaryMessageDecoder<GtidKeyItem> getDecoder() {
        return DECODER;
    }

    /**
     * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
     * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
     * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
     */
    public static BinaryMessageDecoder<GtidKeyItem> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
    }

    /**
     * Serializes this GtidKeyItemAvro to a ByteBuffer.
     * @return a buffer holding the serialized data for this instance
     * @throws java.io.IOException if this instance could not be serialized
     */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
        return ENCODER.encode(this);
    }

    /**
     * Deserializes a GtidKeyItemAvro from a ByteBuffer.
     * @param b a byte buffer holding serialized data for an instance of this class
     * @return a GtidKeyItemAvro instance decoded from the given buffer
     * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
     */
    public static GtidKeyItem fromByteBuffer(
            java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
    }

    private CharSequence uuid;
    private CharSequence cmd;
    private CharSequence address;
    private CharSequence seq;
    private java.util.List<Integer> key;
    private java.util.List<Integer> subkey;
    private CharSequence dbid;
    private Long timestamp;
    private int shardid;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public GtidKeyItem() {}

    /**
     * All-args constructor.
     * @param uuid The new value for uuid
     * @param cmd The new value for cmd
     * @param address The new value for address
     * @param seq The new value for seq
     * @param key The new value for key
     * @param subkey The new value for subkey
     * @param dbid The new value for dbid
     * @param timestamp The new value for timestamp
     * @param shardid The new value for shardid
     */
    public GtidKeyItem(CharSequence uuid, CharSequence cmd, CharSequence address, CharSequence seq, java.util.List<Integer> key, java.util.List<Integer> subkey, CharSequence dbid, Long timestamp, Integer shardid) {
        this.uuid = uuid;
        this.cmd = cmd;
        this.address = address;
        this.seq = seq;
        this.key = key;
        this.subkey = subkey;
        this.dbid = dbid;
        this.timestamp = timestamp;
        this.shardid = shardid;
    }

    @Override
    public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

    @Override
    public org.apache.avro.Schema getSchema() { return SCHEMA$; }

    // Used by DatumWriter.  Applications should not call.
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return uuid;
            case 1: return cmd;
            case 2: return address;
            case 3: return seq;
            case 4: return key;
            case 5: return subkey;
            case 6: return dbid;
            case 7: return timestamp;
            case 8: return shardid;
            default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    // Used by DatumReader.  Applications should not call.
    @Override
    @SuppressWarnings(value="unchecked")
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: uuid = (CharSequence)value$; break;
            case 1: cmd = (CharSequence)value$; break;
            case 2: address = (CharSequence)value$; break;
            case 3: seq = (CharSequence)value$; break;
            case 4: key = (java.util.List<Integer>)value$; break;
            case 5: subkey = (java.util.List<Integer>)value$; break;
            case 6: dbid = (CharSequence)value$; break;
            case 7: timestamp = (Long)value$; break;
            case 8: shardid = (Integer)value$; break;
            default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    /**
     * Gets the value of the 'uuid' field.
     * @return The value of the 'uuid' field.
     */
    public CharSequence getUuid() {
        return uuid;
    }


    /**
     * Sets the value of the 'uuid' field.
     * @param value the value to set.
     */
    public void setUuid(CharSequence value) {
        this.uuid = value;
    }

    /**
     * Gets the value of the 'cmd' field.
     * @return The value of the 'cmd' field.
     */
    public CharSequence getCmd() {
        return cmd;
    }


    /**
     * Sets the value of the 'cmd' field.
     * @param value the value to set.
     */
    public void setCmd(CharSequence value) {
        this.cmd = value;
    }

    /**
     * Gets the value of the 'address' field.
     * @return The value of the 'address' field.
     */
    public CharSequence getAddress() {
        return address;
    }


    /**
     * Sets the value of the 'address' field.
     * @param value the value to set.
     */
    public void setAddress(CharSequence value) {
        this.address = value;
    }

    /**
     * Gets the value of the 'seq' field.
     * @return The value of the 'seq' field.
     */
    public CharSequence getSeq() {
        return seq;
    }


    /**
     * Sets the value of the 'seq' field.
     * @param value the value to set.
     */
    public void setSeq(CharSequence value) {
        this.seq = value;
    }

    /**
     * Gets the value of the 'key' field.
     * @return The value of the 'key' field.
     */
    public java.util.List<Integer> getKey() {
        return key;
    }


    /**
     * Sets the value of the 'key' field.
     * @param value the value to set.
     */
    public void setKey(java.util.List<Integer> value) {
        this.key = value;
    }

    /**
     * Gets the value of the 'subkey' field.
     * @return The value of the 'subkey' field.
     */
    public java.util.List<Integer> getSubkey() {
        return subkey;
    }


    /**
     * Sets the value of the 'subkey' field.
     * @param value the value to set.
     */
    public void setSubkey(java.util.List<Integer> value) {
        this.subkey = value;
    }

    /**
     * Gets the value of the 'dbid' field.
     * @return The value of the 'dbid' field.
     */
    public CharSequence getDbid() {
        return dbid;
    }


    /**
     * Sets the value of the 'dbid' field.
     * @param value the value to set.
     */
    public void setDbid(CharSequence value) {
        this.dbid = value;
    }

    /**
     * Gets the value of the 'timestamp' field.
     * @return The value of the 'timestamp' field.
     */
    public Long getTimestamp() {
        return timestamp;
    }


    /**
     * Sets the value of the 'timestamp' field.
     * @param value the value to set.
     */
    public void setTimestamp(Long value) {
        this.timestamp = value;
    }

    /**
     * Gets the value of the 'shardid' field.
     * @return The value of the 'shardid' field.
     */
    public int getShardid() {
        return shardid;
    }


    /**
     * Sets the value of the 'shardid' field.
     * @param value the value to set.
     */
    public void setShardid(int value) {
        this.shardid = value;
    }

    /**
     * Creates a new GtidKeyItemAvro RecordBuilder.
     * @return A new GtidKeyItemAvro RecordBuilder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new GtidKeyItemAvro RecordBuilder by copying an existing Builder.
     * @param other The existing builder to copy.
     * @return A new GtidKeyItemAvro RecordBuilder
     */
    public static Builder newBuilder(Builder other) {
        if (other == null) {
            return new Builder();
        } else {
            return new Builder(other);
        }
    }

    /**
     * Creates a new GtidKeyItemAvro RecordBuilder by copying an existing GtidKeyItemAvro instance.
     * @param other The existing instance to copy.
     * @return A new GtidKeyItemAvro RecordBuilder
     */
    public static Builder newBuilder(GtidKeyItem other) {
        if (other == null) {
            return new Builder();
        } else {
            return new Builder(other);
        }
    }

    /**
     * RecordBuilder for GtidKeyItemAvro instances.
     */
    @org.apache.avro.specific.AvroGenerated
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<GtidKeyItem>
            implements org.apache.avro.data.RecordBuilder<GtidKeyItem> {

        private CharSequence uuid;
        private CharSequence cmd;
        private CharSequence address;
        private CharSequence seq;
        private java.util.List<Integer> key;
        private java.util.List<Integer> subkey;
        private CharSequence dbid;
        private Long timestamp;
        private int shardid;

        /** Creates a new Builder */
        private Builder() {
            super(SCHEMA$, MODEL$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         * @param other The existing Builder to copy.
         */
        private Builder(Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.uuid)) {
                this.uuid = data().deepCopy(fields()[0].schema(), other.uuid);
                fieldSetFlags()[0] = other.fieldSetFlags()[0];
            }
            if (isValidValue(fields()[1], other.cmd)) {
                this.cmd = data().deepCopy(fields()[1].schema(), other.cmd);
                fieldSetFlags()[1] = other.fieldSetFlags()[1];
            }
            if (isValidValue(fields()[2], other.address)) {
                this.address = data().deepCopy(fields()[2].schema(), other.address);
                fieldSetFlags()[2] = other.fieldSetFlags()[2];
            }
            if (isValidValue(fields()[3], other.seq)) {
                this.seq = data().deepCopy(fields()[3].schema(), other.seq);
                fieldSetFlags()[3] = other.fieldSetFlags()[3];
            }
            if (isValidValue(fields()[4], other.key)) {
                this.key = data().deepCopy(fields()[4].schema(), other.key);
                fieldSetFlags()[4] = other.fieldSetFlags()[4];
            }
            if (isValidValue(fields()[5], other.subkey)) {
                this.subkey = data().deepCopy(fields()[5].schema(), other.subkey);
                fieldSetFlags()[5] = other.fieldSetFlags()[5];
            }
            if (isValidValue(fields()[6], other.dbid)) {
                this.dbid = data().deepCopy(fields()[6].schema(), other.dbid);
                fieldSetFlags()[6] = other.fieldSetFlags()[6];
            }
            if (isValidValue(fields()[7], other.timestamp)) {
                this.timestamp = data().deepCopy(fields()[7].schema(), other.timestamp);
                fieldSetFlags()[7] = other.fieldSetFlags()[7];
            }
            if (isValidValue(fields()[8], other.shardid)) {
                this.shardid = data().deepCopy(fields()[8].schema(), other.shardid);
                fieldSetFlags()[8] = other.fieldSetFlags()[8];
            }
        }

        /**
         * Creates a Builder by copying an existing GtidKeyItemAvro instance
         * @param other The existing instance to copy.
         */
        private Builder(GtidKeyItem other) {
            super(SCHEMA$, MODEL$);
            if (isValidValue(fields()[0], other.uuid)) {
                this.uuid = data().deepCopy(fields()[0].schema(), other.uuid);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.cmd)) {
                this.cmd = data().deepCopy(fields()[1].schema(), other.cmd);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.address)) {
                this.address = data().deepCopy(fields()[2].schema(), other.address);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.seq)) {
                this.seq = data().deepCopy(fields()[3].schema(), other.seq);
                fieldSetFlags()[3] = true;
            }
            if (isValidValue(fields()[4], other.key)) {
                this.key = data().deepCopy(fields()[4].schema(), other.key);
                fieldSetFlags()[4] = true;
            }
            if (isValidValue(fields()[5], other.subkey)) {
                this.subkey = data().deepCopy(fields()[5].schema(), other.subkey);
                fieldSetFlags()[5] = true;
            }
            if (isValidValue(fields()[6], other.dbid)) {
                this.dbid = data().deepCopy(fields()[6].schema(), other.dbid);
                fieldSetFlags()[6] = true;
            }
            if (isValidValue(fields()[7], other.timestamp)) {
                this.timestamp = data().deepCopy(fields()[7].schema(), other.timestamp);
                fieldSetFlags()[7] = true;
            }
            if (isValidValue(fields()[8], other.shardid)) {
                this.shardid = data().deepCopy(fields()[8].schema(), other.shardid);
                fieldSetFlags()[8] = true;
            }
        }

        /**
         * Gets the value of the 'uuid' field.
         * @return The value.
         */
        public CharSequence getUuid() {
            return uuid;
        }


        /**
         * Sets the value of the 'uuid' field.
         * @param value The value of 'uuid'.
         * @return This builder.
         */
        public Builder setUuid(CharSequence value) {
            validate(fields()[0], value);
            this.uuid = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'uuid' field has been set.
         * @return True if the 'uuid' field has been set, false otherwise.
         */
        public boolean hasUuid() {
            return fieldSetFlags()[0];
        }


        /**
         * Clears the value of the 'uuid' field.
         * @return This builder.
         */
        public Builder clearUuid() {
            uuid = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        /**
         * Gets the value of the 'cmd' field.
         * @return The value.
         */
        public CharSequence getCmd() {
            return cmd;
        }


        /**
         * Sets the value of the 'cmd' field.
         * @param value The value of 'cmd'.
         * @return This builder.
         */
        public Builder setCmd(CharSequence value) {
            validate(fields()[1], value);
            this.cmd = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /**
         * Checks whether the 'cmd' field has been set.
         * @return True if the 'cmd' field has been set, false otherwise.
         */
        public boolean hasCmd() {
            return fieldSetFlags()[1];
        }


        /**
         * Clears the value of the 'cmd' field.
         * @return This builder.
         */
        public Builder clearCmd() {
            cmd = null;
            fieldSetFlags()[1] = false;
            return this;
        }

        /**
         * Gets the value of the 'address' field.
         * @return The value.
         */
        public CharSequence getAddress() {
            return address;
        }


        /**
         * Sets the value of the 'address' field.
         * @param value The value of 'address'.
         * @return This builder.
         */
        public Builder setAddress(CharSequence value) {
            validate(fields()[2], value);
            this.address = value;
            fieldSetFlags()[2] = true;
            return this;
        }

        /**
         * Checks whether the 'address' field has been set.
         * @return True if the 'address' field has been set, false otherwise.
         */
        public boolean hasAddress() {
            return fieldSetFlags()[2];
        }


        /**
         * Clears the value of the 'address' field.
         * @return This builder.
         */
        public Builder clearAddress() {
            address = null;
            fieldSetFlags()[2] = false;
            return this;
        }

        /**
         * Gets the value of the 'seq' field.
         * @return The value.
         */
        public CharSequence getSeq() {
            return seq;
        }


        /**
         * Sets the value of the 'seq' field.
         * @param value The value of 'seq'.
         * @return This builder.
         */
        public Builder setSeq(CharSequence value) {
            validate(fields()[3], value);
            this.seq = value;
            fieldSetFlags()[3] = true;
            return this;
        }

        /**
         * Checks whether the 'seq' field has been set.
         * @return True if the 'seq' field has been set, false otherwise.
         */
        public boolean hasSeq() {
            return fieldSetFlags()[3];
        }


        /**
         * Clears the value of the 'seq' field.
         * @return This builder.
         */
        public Builder clearSeq() {
            seq = null;
            fieldSetFlags()[3] = false;
            return this;
        }

        /**
         * Gets the value of the 'key' field.
         * @return The value.
         */
        public java.util.List<Integer> getKey() {
            return key;
        }


        /**
         * Sets the value of the 'key' field.
         * @param value The value of 'key'.
         * @return This builder.
         */
        public Builder setKey(java.util.List<Integer> value) {
            validate(fields()[4], value);
            this.key = value;
            fieldSetFlags()[4] = true;
            return this;
        }

        /**
         * Checks whether the 'key' field has been set.
         * @return True if the 'key' field has been set, false otherwise.
         */
        public boolean hasKey() {
            return fieldSetFlags()[4];
        }


        /**
         * Clears the value of the 'key' field.
         * @return This builder.
         */
        public Builder clearKey() {
            key = null;
            fieldSetFlags()[4] = false;
            return this;
        }

        /**
         * Gets the value of the 'subkey' field.
         * @return The value.
         */
        public java.util.List<Integer> getSubkey() {
            return subkey;
        }


        /**
         * Sets the value of the 'subkey' field.
         * @param value The value of 'subkey'.
         * @return This builder.
         */
        public Builder setSubkey(java.util.List<Integer> value) {
            validate(fields()[5], value);
            this.subkey = value;
            fieldSetFlags()[5] = true;
            return this;
        }

        /**
         * Checks whether the 'subkey' field has been set.
         * @return True if the 'subkey' field has been set, false otherwise.
         */
        public boolean hasSubkey() {
            return fieldSetFlags()[5];
        }


        /**
         * Clears the value of the 'subkey' field.
         * @return This builder.
         */
        public Builder clearSubkey() {
            subkey = null;
            fieldSetFlags()[5] = false;
            return this;
        }

        /**
         * Gets the value of the 'dbid' field.
         * @return The value.
         */
        public CharSequence getDbid() {
            return dbid;
        }


        /**
         * Sets the value of the 'dbid' field.
         * @param value The value of 'dbid'.
         * @return This builder.
         */
        public Builder setDbid(CharSequence value) {
            validate(fields()[6], value);
            this.dbid = value;
            fieldSetFlags()[6] = true;
            return this;
        }

        /**
         * Checks whether the 'dbid' field has been set.
         * @return True if the 'dbid' field has been set, false otherwise.
         */
        public boolean hasDbid() {
            return fieldSetFlags()[6];
        }


        /**
         * Clears the value of the 'dbid' field.
         * @return This builder.
         */
        public Builder clearDbid() {
            dbid = null;
            fieldSetFlags()[6] = false;
            return this;
        }

        /**
         * Gets the value of the 'timestamp' field.
         * @return The value.
         */
        public Long getTimestamp() {
            return timestamp;
        }


        /**
         * Sets the value of the 'timestamp' field.
         * @param value The value of 'timestamp'.
         * @return This builder.
         */
        public Builder setTimestamp(Long value) {
            validate(fields()[7], value);
            this.timestamp = value;
            fieldSetFlags()[7] = true;
            return this;
        }

        /**
         * Checks whether the 'timestamp' field has been set.
         * @return True if the 'timestamp' field has been set, false otherwise.
         */
        public boolean hasTimestamp() {
            return fieldSetFlags()[7];
        }


        /**
         * Clears the value of the 'timestamp' field.
         * @return This builder.
         */
        public Builder clearTimestamp() {
            timestamp = null;
            fieldSetFlags()[7] = false;
            return this;
        }

        /**
         * Gets the value of the 'shardid' field.
         * @return The value.
         */
        public int getShardid() {
            return shardid;
        }


        /**
         * Sets the value of the 'shardid' field.
         * @param value The value of 'shardid'.
         * @return This builder.
         */
        public Builder setShardid(int value) {
            validate(fields()[8], value);
            this.shardid = value;
            fieldSetFlags()[8] = true;
            return this;
        }

        /**
         * Checks whether the 'shardid' field has been set.
         * @return True if the 'shardid' field has been set, false otherwise.
         */
        public boolean hasShardid() {
            return fieldSetFlags()[8];
        }


        /**
         * Clears the value of the 'shardid' field.
         * @return This builder.
         */
        public Builder clearShardid() {
            fieldSetFlags()[8] = false;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public GtidKeyItem build() {
            try {
                GtidKeyItem record = new GtidKeyItem();
                record.uuid = fieldSetFlags()[0] ? this.uuid : (CharSequence) defaultValue(fields()[0]);
                record.cmd = fieldSetFlags()[1] ? this.cmd : (CharSequence) defaultValue(fields()[1]);
                record.address = fieldSetFlags()[2] ? this.address : (CharSequence) defaultValue(fields()[2]);
                record.seq = fieldSetFlags()[3] ? this.seq : (CharSequence) defaultValue(fields()[3]);
                record.key = fieldSetFlags()[4] ? this.key : (java.util.List<Integer>) defaultValue(fields()[4]);
                record.subkey = fieldSetFlags()[5] ? this.subkey : (java.util.List<Integer>) defaultValue(fields()[5]);
                record.dbid = fieldSetFlags()[6] ? this.dbid : (CharSequence) defaultValue(fields()[6]);
                record.timestamp = fieldSetFlags()[7] ? this.timestamp : (Long) defaultValue(fields()[7]);
                record.shardid = fieldSetFlags()[8] ? this.shardid : (Integer) defaultValue(fields()[8]);
                return record;
            } catch (org.apache.avro.AvroMissingFieldException e) {
                throw e;
            } catch (Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<GtidKeyItem>
            WRITER$ = (org.apache.avro.io.DatumWriter<GtidKeyItem>)MODEL$.createDatumWriter(SCHEMA$);

    @Override public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<GtidKeyItem>
            READER$ = (org.apache.avro.io.DatumReader<GtidKeyItem>)MODEL$.createDatumReader(SCHEMA$);

    @Override public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

    @Override protected boolean hasCustomCoders() { return true; }

    @Override public void customEncode(org.apache.avro.io.Encoder out)
            throws java.io.IOException
    {
        out.writeString(this.uuid);

        out.writeString(this.cmd);

        out.writeString(this.address);

        out.writeString(this.seq);

        long size0 = this.key.size();
        out.writeArrayStart();
        out.setItemCount(size0);
        long actualSize0 = 0;
        for (Integer e0: this.key) {
            actualSize0++;
            out.startItem();
            if (e0 == null) {
                out.writeIndex(0);
                out.writeNull();
            } else {
                out.writeIndex(1);
                out.writeInt(e0);
            }
        }
        out.writeArrayEnd();
        if (actualSize0 != size0)
            throw new java.util.ConcurrentModificationException("Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");

        long size1 = this.subkey.size();
        out.writeArrayStart();
        out.setItemCount(size1);
        long actualSize1 = 0;
        for (Integer e1: this.subkey) {
            actualSize1++;
            out.startItem();
            if (e1 == null) {
                out.writeIndex(0);
                out.writeNull();
            } else {
                out.writeIndex(1);
                out.writeInt(e1);
            }
        }
        out.writeArrayEnd();
        if (actualSize1 != size1)
            throw new java.util.ConcurrentModificationException("Array-size written was " + size1 + ", but element count was " + actualSize1 + ".");

        out.writeString(this.dbid);

        if (this.timestamp == null) {
            out.writeIndex(0);
            out.writeNull();
        } else {
            out.writeIndex(1);
            out.writeLong(this.timestamp);
        }

        out.writeInt(this.shardid);

    }

    @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
            throws java.io.IOException
    {
        org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
        if (fieldOrder == null) {
            this.uuid = in.readString(this.uuid instanceof Utf8 ? (Utf8)this.uuid : null);

            this.cmd = in.readString(this.cmd instanceof Utf8 ? (Utf8)this.cmd : null);

            this.address = in.readString(this.address instanceof Utf8 ? (Utf8)this.address : null);

            this.seq = in.readString(this.seq instanceof Utf8 ? (Utf8)this.seq : null);

            long size0 = in.readArrayStart();
            java.util.List<Integer> a0 = this.key;
            if (a0 == null) {
                a0 = new SpecificData.Array<Integer>((int)size0, SCHEMA$.getField("key").schema());
                this.key = a0;
            } else a0.clear();
            SpecificData.Array<Integer> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<Integer>)a0 : null);
            for ( ; 0 < size0; size0 = in.arrayNext()) {
                for ( ; size0 != 0; size0--) {
                    Integer e0 = (ga0 != null ? ga0.peek() : null);
                    if (in.readIndex() != 1) {
                        in.readNull();
                        e0 = null;
                    } else {
                        e0 = in.readInt();
                    }
                    a0.add(e0);
                }
            }

            long size1 = in.readArrayStart();
            java.util.List<Integer> a1 = this.subkey;
            if (a1 == null) {
                a1 = new SpecificData.Array<Integer>((int)size1, SCHEMA$.getField("subkey").schema());
                this.subkey = a1;
            } else a1.clear();
            SpecificData.Array<Integer> ga1 = (a1 instanceof SpecificData.Array ? (SpecificData.Array<Integer>)a1 : null);
            for ( ; 0 < size1; size1 = in.arrayNext()) {
                for ( ; size1 != 0; size1--) {
                    Integer e1 = (ga1 != null ? ga1.peek() : null);
                    if (in.readIndex() != 1) {
                        in.readNull();
                        e1 = null;
                    } else {
                        e1 = in.readInt();
                    }
                    a1.add(e1);
                }
            }

            this.dbid = in.readString(this.dbid instanceof Utf8 ? (Utf8)this.dbid : null);

            if (in.readIndex() != 1) {
                in.readNull();
                this.timestamp = null;
            } else {
                this.timestamp = in.readLong();
            }

            this.shardid = in.readInt();

        } else {
            for (int i = 0; i < 9; i++) {
                switch (fieldOrder[i].pos()) {
                    case 0:
                        this.uuid = in.readString(this.uuid instanceof Utf8 ? (Utf8)this.uuid : null);
                        break;

                    case 1:
                        this.cmd = in.readString(this.cmd instanceof Utf8 ? (Utf8)this.cmd : null);
                        break;

                    case 2:
                        this.address = in.readString(this.address instanceof Utf8 ? (Utf8)this.address : null);
                        break;

                    case 3:
                        this.seq = in.readString(this.seq instanceof Utf8 ? (Utf8)this.seq : null);
                        break;

                    case 4:
                        long size0 = in.readArrayStart();
                        java.util.List<Integer> a0 = this.key;
                        if (a0 == null) {
                            a0 = new SpecificData.Array<Integer>((int)size0, SCHEMA$.getField("key").schema());
                            this.key = a0;
                        } else a0.clear();
                        SpecificData.Array<Integer> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<Integer>)a0 : null);
                        for ( ; 0 < size0; size0 = in.arrayNext()) {
                            for ( ; size0 != 0; size0--) {
                                Integer e0 = (ga0 != null ? ga0.peek() : null);
                                if (in.readIndex() != 1) {
                                    in.readNull();
                                    e0 = null;
                                } else {
                                    e0 = in.readInt();
                                }
                                a0.add(e0);
                            }
                        }
                        break;

                    case 5:
                        long size1 = in.readArrayStart();
                        java.util.List<Integer> a1 = this.subkey;
                        if (a1 == null) {
                            a1 = new SpecificData.Array<Integer>((int)size1, SCHEMA$.getField("subkey").schema());
                            this.subkey = a1;
                        } else a1.clear();
                        SpecificData.Array<Integer> ga1 = (a1 instanceof SpecificData.Array ? (SpecificData.Array<Integer>)a1 : null);
                        for ( ; 0 < size1; size1 = in.arrayNext()) {
                            for ( ; size1 != 0; size1--) {
                                Integer e1 = (ga1 != null ? ga1.peek() : null);
                                if (in.readIndex() != 1) {
                                    in.readNull();
                                    e1 = null;
                                } else {
                                    e1 = in.readInt();
                                }
                                a1.add(e1);
                            }
                        }
                        break;

                    case 6:
                        this.dbid = in.readString(this.dbid instanceof Utf8 ? (Utf8)this.dbid : null);
                        break;

                    case 7:
                        if (in.readIndex() != 1) {
                            in.readNull();
                            this.timestamp = null;
                        } else {
                            this.timestamp = in.readLong();
                        }
                        break;

                    case 8:
                        this.shardid = in.readInt();
                        break;

                    default:
                        throw new java.io.IOException("Corrupt ResolvingDecoder.");
                }
            }
        }
    }

    public static List<Integer> getKeyList(byte[] key){
        List<Integer> keyList = new ArrayList<>(key.length);
        for(int i = 0;i<key.length;i++){
            keyList.add((int) key[i]);
        }
        return keyList;
    }

    public static GtidKeyItem buildGtidKeyItem(String cmd, String uuid, String seq, byte[] key, byte[] subkey, String dbId, long shardId, String address){
        List<Integer> subKeyList = new ArrayList<>();
        if(subkey != null){
            subKeyList = getKeyList(subkey);
        }
        return new GtidKeyItem(uuid,cmd,address,seq,getKeyList(key),subKeyList,dbId,System.currentTimeMillis()/1000,(int)shardId);
//        GtidKeyItem.Builder builder = GtidKeyItem
//                .newBuilder()
//                .setUuid(uuid)
//                .setSeq(seq)
//                .setDbid(dbId)
//                .setCmd(cmd)
//                .setKey(keyList)
//                .setAddress(address)
//                .setShardid((int)shardId)
//                .setTimestamp(System.currentTimeMillis()/1000);
//        if(subKeyList != null){
//            builder.setSubkey(subKeyList);
//        }
//        return builder.build();
    }
}











