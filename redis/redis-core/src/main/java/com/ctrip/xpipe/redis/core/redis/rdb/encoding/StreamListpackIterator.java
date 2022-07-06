package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.redis.core.redis.exception.RdbStreamParseFailException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lishanglin
 * date 2022/6/23
 */
public class StreamListpackIterator {

    private StreamID masterId;

    private Listpack listpack;

    private AtomicInteger cursor;

    private long size;

    private long deletedSize;

    private long masterFieldsSize;

    private List<byte[]> masterFields;

    private static final byte STREAM_ITEM_FLAG_NONE = 0;
    private static final byte STREAM_ITEM_FLAG_DELETED = 0b01;
    private static final byte STREAM_ITEM_FLAG_SAMEFIELDS = 0b10;

    public StreamListpackIterator(StreamID masterId, Listpack listpack) {
        this.masterId = masterId;
        this.listpack = listpack;
        this.cursor = new AtomicInteger(0);
        this.decodeHeader();
    }

    private void decodeHeader() {
        this.size = listpack.getElement(0).getInt();
        this.deletedSize = listpack.getElement(1).getInt();
        this.masterFieldsSize = listpack.getElement(2).getInt();
        this.cursor.set(3);

        this.masterFields = new LinkedList<>();
        while (cursor.get() < masterFieldsSize + 3) {
            this.masterFields.add(listpack.getElement(cursor.getAndIncrement()).getBytes());
        }

        this.cursor.incrementAndGet(); // skip master term
    }

    public boolean hasNext() {
        return this.cursor.get() < this.listpack.size();
    }

    public StreamEntry next() {
        if (!hasNext()) throw new RdbStreamParseFailException("no more data");

        long flag = listpack.getElement(cursor.getAndIncrement()).getInt();
        long relatedMs = listpack.getElement(cursor.getAndIncrement()).getInt();
        long relatedSeq = listpack.getElement(cursor.getAndIncrement()).getInt();

        StreamEntry streamEntry = new StreamEntry(
                new StreamID(masterId.getMs() + relatedMs, masterId.getSeq() + relatedSeq),
                0 != (flag & STREAM_ITEM_FLAG_DELETED));

        boolean sameFields = 0 != (flag & STREAM_ITEM_FLAG_SAMEFIELDS);
        long fieldsSize;
        if (sameFields) {
            fieldsSize = masterFieldsSize;
        } else {
            fieldsSize = listpack.getElement(cursor.getAndIncrement()).getInt();
        }

        for (int i = 0; i < fieldsSize; i++) {
            if (sameFields) {
                byte[] value = listpack.getElement(cursor.getAndIncrement()).getBytes();
                streamEntry.addField(masterFields.get(i), value);
            } else {
                byte[] field = listpack.getElement(cursor.getAndIncrement()).getBytes();
                byte[] value = listpack.getElement(cursor.getAndIncrement()).getBytes();
                streamEntry.addField(field, value);
            }
        }
        this.cursor.incrementAndGet(); // skip prev entry lp-count

        return streamEntry;
    }

}
