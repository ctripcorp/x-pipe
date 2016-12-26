package com.ctrip.xpipe.redis.core.util;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author marsqing
 *
 *         Dec 26, 2016 12:02:37 PM
 */
public class NonFinalizeFileInputStream extends FileInputStream {

	public NonFinalizeFileInputStream(File file) throws FileNotFoundException {
		super(file);
	}

	public NonFinalizeFileInputStream(FileDescriptor fdObj) {
		super(fdObj);
	}

	public NonFinalizeFileInputStream(String name) throws FileNotFoundException {
		super(name);
	}

	/**
	 * Suppress finalize() to avoid slow ygc
	 */
	@Override
	protected void finalize() throws IOException {
	}

}
