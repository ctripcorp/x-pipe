package com.ctrip.xpipe.redis.console.ds;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import org.codehaus.plexus.logging.AbstractLogger;
import org.codehaus.plexus.logging.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.unidal.dal.jdbc.DalRuntimeException;
import org.unidal.dal.jdbc.DataField;
import org.unidal.dal.jdbc.entity.DataObjectAccessor;
import org.unidal.dal.jdbc.entity.DefaultDataObjectAccessor;
import org.unidal.dal.jdbc.entity.DefaultDataObjectNaming;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class XPipeDataObjectAccessorTest {

    private static final String CREATE_TIME_FIELD = "create-time";

    @Test
    public void setCreateTimeFromLocalDateTime2040() throws Exception {
        LocalDateTime fromDb = LocalDateTime.of(2040, 1, 15, 10, 20, 30);
        RedisTbl row = setCreateTime(newAccessor(), fromDb);
        Assert.assertEquals(Timestamp.valueOf(fromDb), row.getCreateTime());
    }

    @Test
    public void setCreateTimeFromLocalDateTimeNow() throws Exception {
        LocalDateTime fromDb = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        RedisTbl row = setCreateTime(newAccessor(), fromDb);
        Assert.assertEquals(Timestamp.valueOf(fromDb), row.getCreateTime());
    }

    @Test
    public void setCreateTimeFromNull() throws Exception {
        RedisTbl row = setCreateTime(newAccessor(), null);
        Assert.assertNull(row.getCreateTime());
    }

    @Test
    public void setCreateTimeFromDate() throws Exception {
        Date expected = new Date();
        RedisTbl row = setCreateTime(newAccessor(), expected);
        Assert.assertSame(expected, row.getCreateTime());
    }

    @Test
    public void setCreateTimeFromTimestamp() throws Exception {
        Timestamp expected = Timestamp.valueOf(LocalDateTime.of(2023, 11, 14, 22, 13, 20));
        RedisTbl row = setCreateTime(newAccessor(), expected);
        Assert.assertSame(expected, row.getCreateTime());
    }

    @Test
    public void setCreateTimeFromLocalDate() throws Exception {
        LocalDate fromDb = LocalDate.of(2024, 6, 18);
        RedisTbl row = setCreateTime(newAccessor(), fromDb);
        Assert.assertEquals(java.sql.Date.valueOf(fromDb), row.getCreateTime());
    }

    @Test(expected = DalRuntimeException.class)
    public void defaultAccessorRejectsLocalDateTime() throws Exception {
        DefaultDataObjectAccessor accessor = new DefaultDataObjectAccessor();
        accessor.enableLogging(new NullLogger(Logger.LEVEL_ERROR, "defaultAccessor"));
        LocalDateTime fromDb = LocalDateTime.of(2040, 1, 15, 10, 20, 30);
        setCreateTime(accessor, fromDb);
    }

    private static RedisTbl setCreateTime(DataObjectAccessor accessor, Object value) throws Exception {
        initNaming(accessor);
        RedisTbl row = new RedisTbl();
        DataField field = new DataField(CREATE_TIME_FIELD);
        field.setEntityClass(RedisTbl.class);
        accessor.setFieldValue(row, field, value);
        return row;
    }

    private static XPipeDataObjectAccessor newAccessor() {
        return new XPipeDataObjectAccessor();
    }

    private static void initNaming(DataObjectAccessor accessor) throws Exception {
        Field naming = DefaultDataObjectAccessor.class.getDeclaredField("m_naming");
        naming.setAccessible(true);
        naming.set(accessor, new DefaultDataObjectNaming());
    }

    private static final class NullLogger extends AbstractLogger {

        private NullLogger(int threshold, String name) {
            super(threshold, name);
        }

        @Override
        public void debug(String message, Throwable throwable) {
        }

        @Override
        public void info(String message, Throwable throwable) {
        }

        @Override
        public void warn(String message, Throwable throwable) {
        }

        @Override
        public void error(String message, Throwable throwable) {
        }

        @Override
        public void fatalError(String message, Throwable throwable) {
        }

        @Override
        public Logger getChildLogger(String name) {
            return this;
        }
    }
}
