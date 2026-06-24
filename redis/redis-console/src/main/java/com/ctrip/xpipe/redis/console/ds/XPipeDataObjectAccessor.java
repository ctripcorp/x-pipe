package com.ctrip.xpipe.redis.console.ds;

import org.unidal.dal.jdbc.entity.DefaultDataObjectAccessor;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * Unidal DAL accessor that maps MySQL {@code DATETIME} columns to {@link Date}.
 * MySQL Connector/J 8.0 returns {@link LocalDateTime} for DATETIME via {@code ResultSet.getObject()},
 * which the default accessor does not convert.
 */
public class XPipeDataObjectAccessor extends DefaultDataObjectAccessor {

    @Override
    protected Object convert(Object value, Class<?> targetType) {
        if (value != null && Date.class.isAssignableFrom(targetType)) {
            if (value instanceof LocalDateTime) {
                return Timestamp.valueOf((LocalDateTime) value);
            }
            if (value instanceof LocalDate) {
                return java.sql.Date.valueOf((LocalDate) value);
            }
        }
        return super.convert(value, targetType);
    }
}
