package com.ctrip.xpipe.redis.core.util;

import java.io.*;

/**
 * @author marsqing
 *
 *         Dec 26, 2016 12:05:03 PM
 */
public class NonFinalizeFileOutputStream extends FileOutputStream {

	public NonFinalizeFileOutputStream(File file, boolean append) throws FileNotFoundException {
		super(file, append);
	}

	public NonFinalizeFileOutputStream(File file) throws FileNotFoundException {
		super(file);
	}

	public NonFinalizeFileOutputStream(FileDescriptor fdObj) {
		super(fdObj);
	}

	public NonFinalizeFileOutputStream(String name, boolean append) throws FileNotFoundException {
		super(name, append);
	}

	public NonFinalizeFileOutputStream(String name) throws FileNotFoundException {
		super(name);
	}

	/**
	 * Suppress finalize() to avoid slow ygc
	 */
	@Override
	protected void finalize() throws IOException {
	}

}
