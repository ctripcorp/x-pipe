package com.ctrip.xpipe.payload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午4:33:20
 */
public class FileInOutPayload extends AbstractInOutPayload{
	
	public String fileName;
	
	private InputStream ins;
	
	private OutputStream ous;
	
	public FileInOutPayload(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public InputStream getInputStream() {
		return ins;
	}

	@Override
	public OutputStream getOutputStream() {
		return ous;
	}

	@Override
	public void startInputStream() {
		
		try {
			ins = new FileInputStream(new File(fileName));
		} catch (FileNotFoundException e) {
			throw new XpipeRuntimeException("file:" + fileName, e);
		}
		
	}

	@Override
	public void endInputStream() {
		if( ins != null ){
			try {
				ins.close();
			} catch (IOException e) {
				logger.error("[endInputStream]" + fileName, e);
			}
		}
	}

	@Override
	public void startOutputStream() {
		try {
			ous = new FileOutputStream(new File(fileName));
		} catch (FileNotFoundException e) {
			throw new XpipeRuntimeException("file:" + fileName, e);
		}
	}

	@Override
	public void endOutputStream() {
		if(ous != null){
			try {
				ous.close();
			} catch (IOException e) {
				logger.error("[endOutputStream]" + fileName, e);
			}
		}
	}
}
