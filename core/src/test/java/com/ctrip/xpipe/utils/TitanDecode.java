package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *
 * Oct 13, 2016
 */

import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

public class TitanDecode {

	@Test
	public void decode() {

		String datasource = "=";
		System.out.println(decrypt(datasource));
	}

	private String decrypt(String dataSource) {

		if (dataSource == null || dataSource.length() == 0) {

			return "";

		}

		byte[] sources = Base64.decodeBase64(dataSource.getBytes(java.nio.charset.StandardCharsets.UTF_8));

		int dataLen = sources.length;

		int keyLen = (int) sources[0];

		int len = dataLen - keyLen - 1;

		byte[] datas = new byte[len];

		int offset = dataLen - 1;

		int i = 0;

		int j = 0;

		byte t;

		for (int o = 0; o < len; o++) {

			i = (i + 1) % keyLen;

			j = (j + sources[offset - i]) % keyLen;

			t = sources[offset - i];

			sources[offset - i] = sources[offset - j];

			sources[offset - j] = t;

			datas[o] = (byte) (sources[o + 1]
					^ sources[offset - ((sources[offset - i] + sources[offset - j]) % keyLen)]);

		}

		return new String(datas);

	}
}
