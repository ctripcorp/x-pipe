package com.ctrip.xpipe.redis.keeper.meta;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.helper.Files.IO;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Function;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:19:44 PM
 */
@Component
public class DefaultMetaService implements MetaService {

	private static Logger logger = LoggerFactory.getLogger(DefaultMetaService.class);

	@Autowired
	private KeeperConfig config;

	@Autowired
	private MetaServerLocator metaServerLocator;

	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId) {
		
		Pair<Integer, String> codeAndRes = getRequestToMetaServer(String.format("/api/v1/%s/%s", clusterId, shardId), null);

		if (codeAndRes == null || codeAndRes.getKey() != 200) {
			logger.error("[getShardStatus]{}", codeAndRes);
			return null;
		} else {
			return JSON.parseObject(codeAndRes.getValue(), ShardStatus.class);
		}
	}


	public Pair<Integer, String> getRequestToMetaServer(final String path, final Map<String, String> requestParams) {
		return pollMetaServer(new Function<String, Pair<Integer, String>>() {

			@Override
			public Pair<Integer, String> apply(String baseUrl) {
				String url = String.format("%s%s", baseUrl, path);
				InputStream is = null;
				try {
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							url = url + "?" + encodedRequestParamStr;
						}

					}

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

					conn.setConnectTimeout(config.getMetaServerConnectTimeout());
					conn.setReadTimeout(config.getMetaServerReadTimeout());
					conn.setRequestMethod("GET");
					conn.connect();

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						return new Pair<Integer, String>(statusCode, IO.INSTANCE.readFrom(is, Charsets.UTF_8.name()));
					} else {
						return new Pair<Integer, String>(statusCode, null);
					}

				} catch (Exception e) {
					// ignore
					if (logger.isDebugEnabled()) {
						logger.debug("Poll meta server error.", e);
					}
					return null;
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}
				}

			}
		});
	}

	@SuppressWarnings("unused")
	private String post(final String path, final Map<String, String> requestParams, final Object payload) {
		Pair<Integer, String> codeAndRes = pollMetaServer(new Function<String, Pair<Integer, String>>() {

			@Override
			public Pair<Integer, String> apply(String baseUrl) {

				String url = String.format("%s%s", baseUrl, path);
				InputStream is = null;
				OutputStream os = null;

				try {
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							url = url + "?" + encodedRequestParamStr;
						}
					}

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

					conn.setConnectTimeout(config.getMetaServerConnectTimeout());
					conn.setReadTimeout(config.getMetaServerReadTimeout());
					conn.setRequestMethod("POST");
					conn.addRequestProperty("content-type", "application/json");

					if (payload != null) {
						conn.setDoOutput(true);
						conn.connect();
						os = conn.getOutputStream();
						os.write(JSON.toJSONBytes(payload));
					} else {
						conn.connect();
					}

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						return new Pair<Integer, String>(statusCode, IO.INSTANCE.readFrom(is, Charsets.UTF_8.name()));
					} else {
						if (logger.isDebugEnabled()) {
							logger.debug("Response error while posting meta server error({url={}, status={}}).", url, statusCode);
						}
						return new Pair<Integer, String>(statusCode, null);
					}

				} catch (Exception e) {
					// ignore
					if (logger.isDebugEnabled()) {
						logger.debug("Post meta server error.", e);
					}
					return null;
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}

					if (os != null) {
						try {
							os.close();
						} catch (Exception e) {
							// ignore it
						}
					}
				}

			}
		});
		return codeAndRes == null ? null : codeAndRes.getValue();
	}

	private Pair<Integer, String> pollMetaServer(Function<String, Pair<Integer, String>> fun) {
		List<String> metaServerList = metaServerLocator.getMetaServerList();

		for (String url : metaServerList) {
			Pair<Integer, String> result = fun.apply(url);
			if (result != null) {
				return result;
			} else {
				continue;
			}
		}

		return null;

	}

	private String encodePropertiesStr(Map<String, String> properties) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			sb.append(URLEncoder.encode(entry.getKey(), Charsets.UTF_8.name()))//
					.append("=")//
					.append(URLEncoder.encode(entry.getValue(), Charsets.UTF_8.name()))//
					.append("&");
		}

		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return null;
		}
	}
	
	public void setConfig(KeeperConfig config) {
		this.config = config;
	}
	
	public void setMetaServerLocator(MetaServerLocator metaServerLocator) {
		this.metaServerLocator = metaServerLocator;
	}

}