package com.ctrip.xpipe.utils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import com.ctrip.xpipe.exception.ExceptionUtils;

/**
 * @author shyin
 *
 *         Sep 23, 2016
 */
public class ExceptionUtilsTest {
	@Test
	public void testGetOriginalException() {
		assertTrue(ExceptionUtils.getOriginalException(new IOException("test")) instanceof IOException);
		assertTrue(ExceptionUtils.getOriginalException(
				new ExecutionException(new IllegalArgumentException("test"))) instanceof IllegalArgumentException);
		assertTrue(ExceptionUtils.getOriginalException(new ExecutionException(null)) instanceof ExecutionException);
		assertTrue(ExceptionUtils.getOriginalException(new ExecutionException(new InvocationTargetException(
				new HttpClientErrorException(HttpStatus.BAD_REQUEST, "test")))) instanceof HttpClientErrorException);
		assertTrue(ExceptionUtils.getOriginalException(
				new ExecutionException(new InvocationTargetException(null))) instanceof InvocationTargetException);
		assertTrue(ExceptionUtils
				.getOriginalException(new InvocationTargetException(new IOException("test"))) instanceof IOException);
		assertTrue(ExceptionUtils
				.getOriginalException(new InvocationTargetException(null)) instanceof InvocationTargetException);

		assertFalse(ExceptionUtils.getOriginalException(
				new InvocationTargetException(new IOException())) instanceof InvocationTargetException);
		assertFalse(ExceptionUtils.getOriginalException(new ExecutionException(
				new InvocationTargetException(new IOException("test")))) instanceof ExecutionException);
	}
}
