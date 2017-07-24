/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 **/

package org.csource.fastdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.csource.common.IniFileReader;
import org.csource.common.MyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global variables
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class ClientGlobal {
	
	private static final Logger logger = LoggerFactory.getLogger(ClientGlobal.class);

	private static Boolean INITED = Boolean.FALSE;

	public static final String CONF_KEY_CONNECT_TIMEOUT = "connect_timeout";
	public static final String CONF_KEY_NETWORK_TIMEOUT = "network_timeout";
	public static final String CONF_KEY_CHARSET = "charset";
	public static final String CONF_KEY_HTTP_ANTI_STEAL_TOKEN = "http.anti_steal_token";
	public static final String CONF_KEY_HTTP_SECRET_KEY = "http.secret_key";
	public static final String CONF_KEY_HTTP_TRACKER_HTTP_PORT = "http.tracker_http_port";
	public static final String CONF_KEY_TRACKER_SERVER = "tracker_server";

	public static final String PROP_KEY_CONNECT_TIMEOUT_IN_SECONDS = "fastdfs.connect_timeout_in_seconds";
	public static final String PROP_KEY_NETWORK_TIMEOUT_IN_SECONDS = "fastdfs.network_timeout_in_seconds";
	public static final String PROP_KEY_CHARSET = "fastdfs.charset";
	public static final String PROP_KEY_HTTP_ANTI_STEAL_TOKEN = "fastdfs.http_anti_steal_token";
	public static final String PROP_KEY_HTTP_SECRET_KEY = "fastdfs.http_secret_key";
	public static final String PROP_KEY_HTTP_TRACKER_HTTP_PORT = "fastdfs.http_tracker_http_port";
	public static final String PROP_KEY_TRACKER_SERVERS = "fastdfs.tracker_servers";

	public static final int DEFAULT_CONNECT_TIMEOUT = 5; // second
	public static final int DEFAULT_NETWORK_TIMEOUT = 30; // second
	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final boolean DEFAULT_HTTP_ANTI_STEAL_TOKEN = false;
	public static final String DEFAULT_HTTP_SECRET_KEY = "FastDFS1234567890";
	public static final int DEFAULT_HTTP_TRACKER_HTTP_PORT = 80;

	public static int g_connect_timeout = DEFAULT_CONNECT_TIMEOUT * 1000; // millisecond
	public static int g_network_timeout = DEFAULT_NETWORK_TIMEOUT * 1000; // millisecond
	public static String g_charset = DEFAULT_CHARSET;
	public static boolean g_anti_steal_token = DEFAULT_HTTP_ANTI_STEAL_TOKEN; // if
																				// anti-steal
																				// token
	public static String g_secret_key = DEFAULT_HTTP_SECRET_KEY; // generage
																	// token
																	// secret
																	// key
	public static int g_tracker_http_port = DEFAULT_HTTP_TRACKER_HTTP_PORT;

	public static TrackerGroup g_tracker_group;

	/**
	 * 新加入的连接池对象， 通过调用init方法启动
	 */
	public static GenericKeyedObjectPool<InetSocketAddress, Socket> SOCKET_POOL;
	
	public static final String CONF_KEY_POOL_CONFIG_FILE = "pool.config.file";
	
	public static final String CONF_KEY_POOL_MINIDLEPERKEY = "pool.min_idle_per_key";
	public static final String CONF_KEY_POOL_MAXIDLEPERKEY = "pool.max_idle_per_key";
	public static final String CONF_KEY_POOL_MAXTOTALPERKEY = "pool.max_total_per_key";
	public static final String CONF_KEY_POOL_MAXTOTAL = "pool.max_total";
	public static final String CONF_KEY_POOL_BLOCKWHENEXHAUSTED = "pool.block_when_exhausted";
	public static final String CONF_KEY_POOL_LIFO = "pool.lifo";
	public static final String CONF_KEY_POOL_FAIRNESS = "pool.fairness";
	public static final String CONF_KEY_POOL_MAXWAITMILLIS = "pool.max_wait_millis";
	public static final String CONF_KEY_POOL_MINEVICTABLEIDLETIMEMILLIS = "pool.min_evictable_idle_time_millis";
	public static final String CONF_KEY_POOL_NUMTESTSPEREVICTIONRUN = "pool.num_tests_per_eviction_run";
	public static final String CONF_KEY_POOL_TESTONCREATE = "pool.test_on_create";
	public static final String CONF_KEY_POOL_TESTONBORROW = "pool.test_on_borrow";
	public static final String CONF_KEY_POOL_TESTONRETURN = "pool.test_on_return";
	public static final String CONF_KEY_POOL_TESTWHILEIDLE = "pool.test_while_idle";
	public static final String CONF_KEY_POOL_TIMEBETWEENEVICTIONRUNSMILLIS = "pool.time_between_eviction_runs_millis";
	
	public static final String DFT_POOL_PROPERTIES = "/socketPoolConfig.properties";
	public static final int DFT_MIN_IDLE_PER_KEY = 16;
	public static final int DFT_MAX_IDLE_PER_KEY = 32;
	public static final int DFT_MAX_TOTAL_PER_KEY = 256;
	public static final int DFT_MAX_TOTAL = -1;
	public static final boolean DFT_BLOCK_WHEN_EXHAUSTED = true;
	public static final boolean DFT_LIFO = true;
	public static final boolean DFT_FAIRNESS = true;
	public static final long DFT_MAX_WAIT_MILLIS = 60*1000l;
	public static final long DFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 30*60*1000l;
	public static final int DFT_NUM_TESTS_PER_EVICTION_RUN = 16;
	public static final boolean DFT_TEST_ON_CREATE = true;
	public static final boolean DFT_TEST_ON_BORROW = true;
	public static final boolean DFT_TEST_ON_RETURN = true;
	public static final boolean DFT_TEST_WHILE_IDLE = true;
	public static final long DFT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 30*1000l;
	
	private ClientGlobal() {
	}

	private static Properties getPoolProperties(String propertyFilePath) {
		String _path = propertyFilePath;
		Properties p = new Properties();
		InputStream is = null;
		if ( !(null != _path && !_path.trim().isEmpty()) )
			_path = DFT_POOL_PROPERTIES;
		try {
			is = ClientGlobal.class.getResourceAsStream(_path);
			p.load(is);
		} catch (FileNotFoundException e) {
			logger.error("无法SocketPool相关配置文件件[{}]", _path, e);
		} catch (IOException e) {
			logger.error("读取SocketPool配置文件[{}]失败", _path, e);
		} finally {
			if ( null != is ) {
				try {
					is.close();
				} catch ( IOException e ) {
					logger.warn("关闭SocketPool配置文件[{}]流失败", _path);
				} finally {
					is = null;
				}
			}
		}
		return p;
	}
	
	private static boolean loadValueAsBoolean(Properties p, String key, boolean defaultValue) {
		boolean _v = defaultValue;
		if ( null == p )
			return _v;
		String _sv = p.getProperty(key);
		if ( null == _sv || "".equals(_sv) ) {
			_v = defaultValue;
		} else {
			_v = Boolean.valueOf(_sv).booleanValue();
		}
		return _v;
	}
	
	private static int loadValueAsInt(Properties p, String key, int defaultValue) {
		int _v = defaultValue;
		if ( null == p )
			return _v;
		String _sv = p.getProperty(key);
		if ( null == _sv || "".equals(_sv) ) {
			_v = defaultValue;
		} else {
			_v = Integer.valueOf(_sv).intValue();
		}
		return _v;
	}
	
	private static long loadValueAsLong(Properties p, String key, long defaultValue) {
		long _v = defaultValue;
		if ( null == p )
			return _v;
		String _sv = p.getProperty(key);
		if ( null == _sv || "".equals(_sv) ) {
			_v = defaultValue;
		} else {
			_v = Long.valueOf(_sv).longValue();
		}
		return _v;
	}
	
	private static void initSocketPool(String propertyFilePath) {
		Properties p = ClientGlobal.getPoolProperties(propertyFilePath);
		SocketPoolFactory factory = new SocketPoolFactory();
		GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
		config.setMinIdlePerKey(loadValueAsInt(p, CONF_KEY_POOL_MINIDLEPERKEY, DFT_MIN_IDLE_PER_KEY));
		config.setMaxIdlePerKey(loadValueAsInt(p, CONF_KEY_POOL_MAXIDLEPERKEY, DFT_MAX_IDLE_PER_KEY));
		config.setMaxTotalPerKey(loadValueAsInt(p, CONF_KEY_POOL_MAXTOTALPERKEY, DFT_MAX_TOTAL_PER_KEY));
		config.setMaxTotal(loadValueAsInt(p, CONF_KEY_POOL_MAXTOTAL, DFT_MAX_TOTAL));
		config.setBlockWhenExhausted(loadValueAsBoolean(p, CONF_KEY_POOL_BLOCKWHENEXHAUSTED, DFT_BLOCK_WHEN_EXHAUSTED));
		config.setLifo(loadValueAsBoolean(p, CONF_KEY_POOL_LIFO, DFT_LIFO));
		config.setFairness(loadValueAsBoolean(p, CONF_KEY_POOL_FAIRNESS, DFT_FAIRNESS));
		config.setMaxWaitMillis(loadValueAsLong(p, CONF_KEY_POOL_MAXWAITMILLIS, DFT_MAX_WAIT_MILLIS));
		config.setMinEvictableIdleTimeMillis(loadValueAsLong(p, CONF_KEY_POOL_MINEVICTABLEIDLETIMEMILLIS, DFT_MIN_EVICTABLE_IDLE_TIME_MILLIS));
		config.setNumTestsPerEvictionRun(loadValueAsInt(p, CONF_KEY_POOL_NUMTESTSPEREVICTIONRUN, DFT_NUM_TESTS_PER_EVICTION_RUN));
		config.setTestOnCreate(loadValueAsBoolean(p, CONF_KEY_POOL_TESTONCREATE, DFT_TEST_ON_CREATE));
		config.setTestOnBorrow(loadValueAsBoolean(p, CONF_KEY_POOL_TESTONBORROW, DFT_TEST_ON_BORROW));
		config.setTestOnReturn(loadValueAsBoolean(p, CONF_KEY_POOL_TESTONRETURN, DFT_TEST_ON_RETURN));
		config.setTestWhileIdle(loadValueAsBoolean(p, CONF_KEY_POOL_TESTWHILEIDLE, DFT_TEST_WHILE_IDLE));
		config.setTimeBetweenEvictionRunsMillis(
				loadValueAsLong(p, CONF_KEY_POOL_TIMEBETWEENEVICTIONRUNSMILLIS, DFT_TIME_BETWEEN_EVICTION_RUNS_MILLIS)
		);
		SOCKET_POOL = new GenericKeyedObjectPool<InetSocketAddress, Socket>(factory, config);
	}
	
	private static void initSocketPool() {
		initSocketPool(null);
	}
	
	public static void showPoolStatus() {
		logger.info("===========SOCKET连接池状态===========");
		logger.info("SOCKET连接池当前共创建[{}]个链接", SOCKET_POOL.getCreatedCount());
		logger.info("SOCKET连接池借出链接次数[{}]", SOCKET_POOL.getBorrowedCount());
		logger.info("SOCKET连接池总共摧毁链接对象次数[{}]", SOCKET_POOL.getDestroyedCount());
		logger.info("SOCKET连接池因测试未通过摧毁链接对象次数[{}]", SOCKET_POOL.getDestroyedByBorrowValidationCount());
		logger.info("SOCKET连接池因链接未使用检测摧毁链接对象次数[{}]", SOCKET_POOL.getDestroyedByEvictorCount());
		logger.info("SOCKET连接池当前共借出链接对象个数[{}]", SOCKET_POOL.getNumActive());
		Map<String, Integer> numActivePerKey = SOCKET_POOL.getNumActivePerKey();
		for ( Entry<String, Integer> e : numActivePerKey.entrySet() ) {
			logger.info("SOCKET连接池中的子连接池[{}]有[{}]个链接未回收", e.getKey(), e.getValue());
		}
		logger.info("SOCKET连接池当前共有[{}]个链接对象", SOCKET_POOL.getNumIdle());
		logger.info("SOCKET连接池当前共有[{}]个线程在等待获取链接", SOCKET_POOL.getNumWaiters());
		Map<String, Integer> numWaitersPerKey = SOCKET_POOL.getNumWaitersByKey();
		for ( Entry<String, Integer> e : numWaitersPerKey.entrySet() ) {
			logger.info("SOCKET连接池中的子连接池[{}]有[{}]个线程在等待获取链接", e.getKey(), e.getValue());
		}
		logger.info("SOCKET连接池当前共回收过[{}]个链接对象", SOCKET_POOL.getReturnedCount());
	}

	/**
	 * load global variables
	 *
	 * @param conf_filename
	 *            config filename
	 */
	public static void init(String conf_filename) throws IOException, MyException {
		synchronized (INITED) {
			if ( Boolean.FALSE.equals(INITED) ) {
				IniFileReader iniReader;
				String[] szTrackerServers;
				String[] parts;

				iniReader = new IniFileReader(conf_filename);

				g_connect_timeout = iniReader.getIntValue("connect_timeout", DEFAULT_CONNECT_TIMEOUT);
				if (g_connect_timeout < 0) {
					g_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
				}
				g_connect_timeout *= 1000; // millisecond

				g_network_timeout = iniReader.getIntValue("network_timeout", DEFAULT_NETWORK_TIMEOUT);
				if (g_network_timeout < 0) {
					g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
				}
				g_network_timeout *= 1000; // millisecond

				g_charset = iniReader.getStrValue("charset");
				if (g_charset == null || g_charset.length() == 0) {
					g_charset = "UTF-8";
				}

				szTrackerServers = iniReader.getValues("tracker_server");
				if (szTrackerServers == null) {
					throw new MyException("item \"tracker_server\" in " + conf_filename + " not found");
				}

				InetSocketAddress[] tracker_servers = new InetSocketAddress[szTrackerServers.length];
				for (int i = 0; i < szTrackerServers.length; i++) {
					parts = szTrackerServers[i].split("\\:", 2);
					if (parts.length != 2) {
						throw new MyException(
								"the value of item \"tracker_server\" is invalid, the correct format is host:port");
					}

					tracker_servers[i] = new InetSocketAddress(parts[0].trim(), Integer.parseInt(parts[1].trim()));
				}
				g_tracker_group = new TrackerGroup(tracker_servers);

				g_tracker_http_port = iniReader.getIntValue("http.tracker_http_port", 80);
				g_anti_steal_token = iniReader.getBoolValue("http.anti_steal_token", false);
				if (g_anti_steal_token) {
					g_secret_key = iniReader.getStrValue("http.secret_key");
				}
				String poolConfigFile = iniReader.getStrValue(CONF_KEY_POOL_CONFIG_FILE);
				initSocketPool(poolConfigFile);
				INITED = Boolean.TRUE;
			}
		}
	}

	/**
	 * load from properties file
	 *
	 * @param propsFilePath
	 *            properties file path, eg: "fastdfs-client.properties"
	 *            "config/fastdfs-client.properties"
	 *            "/opt/fastdfs-client.properties"
	 *            "C:\\Users\\James\\config\\fastdfs-client.properties"
	 *            properties文件至少包含一个配置项 fastdfs.tracker_servers 例如：
	 *            fastdfs.tracker_servers = 10.0.11.245:22122,10.0.11.246:22122
	 *            server的IP和端口用冒号':'分隔 server之间用逗号','分隔
	 */
	public static void initByProperties(String propsFilePath) throws IOException, MyException {
		synchronized (INITED) {
			if ( Boolean.FALSE.equals(INITED) ) {
				Properties props = new Properties();
				InputStream in = IniFileReader.loadFromOsFileSystemOrClasspathAsStream(propsFilePath);
				if (in != null) {
					props.load(in);
				}
				initByProperties(props);
			}
		}
	}

	public static void initByProperties(Properties props) throws IOException, MyException {
		synchronized (INITED) {
			if ( Boolean.FALSE.equals(INITED) ) {
				String trackerServersConf = props.getProperty(PROP_KEY_TRACKER_SERVERS);
				if (trackerServersConf == null || trackerServersConf.trim().length() == 0) {
					throw new MyException(String.format("configure item %s is required", PROP_KEY_TRACKER_SERVERS));
				}
				initByTrackers(trackerServersConf.trim());

				String connectTimeoutInSecondsConf = props.getProperty(PROP_KEY_CONNECT_TIMEOUT_IN_SECONDS);
				String networkTimeoutInSecondsConf = props.getProperty(PROP_KEY_NETWORK_TIMEOUT_IN_SECONDS);
				String charsetConf = props.getProperty(PROP_KEY_CHARSET);
				String httpAntiStealTokenConf = props.getProperty(PROP_KEY_HTTP_ANTI_STEAL_TOKEN);
				String httpSecretKeyConf = props.getProperty(PROP_KEY_HTTP_SECRET_KEY);
				String httpTrackerHttpPortConf = props.getProperty(PROP_KEY_HTTP_TRACKER_HTTP_PORT);
				String poolConfigFilePath = props.getProperty(CONF_KEY_POOL_CONFIG_FILE);
				if (connectTimeoutInSecondsConf != null && connectTimeoutInSecondsConf.trim().length() != 0) {
					g_connect_timeout = Integer.parseInt(connectTimeoutInSecondsConf.trim()) * 1000;
				}
				if (networkTimeoutInSecondsConf != null && networkTimeoutInSecondsConf.trim().length() != 0) {
					g_network_timeout = Integer.parseInt(networkTimeoutInSecondsConf.trim()) * 1000;
				}
				if (charsetConf != null && charsetConf.trim().length() != 0) {
					g_charset = charsetConf.trim();
				}
				if (httpAntiStealTokenConf != null && httpAntiStealTokenConf.trim().length() != 0) {
					g_anti_steal_token = Boolean.parseBoolean(httpAntiStealTokenConf);
				}
				if (httpSecretKeyConf != null && httpSecretKeyConf.trim().length() != 0) {
					g_secret_key = httpSecretKeyConf.trim();
				}
				if (httpTrackerHttpPortConf != null && httpTrackerHttpPortConf.trim().length() != 0) {
					g_tracker_http_port = Integer.parseInt(httpTrackerHttpPortConf);
				}
				initSocketPool(poolConfigFilePath);
				INITED = Boolean.TRUE;
			} 
		}
	}

	/**
	 * load from properties file
	 *
	 * @param trackerServers
	 *            例如："10.0.11.245:22122,10.0.11.246:22122" server的IP和端口用冒号':'分隔
	 *            server之间用逗号','分隔
	 */
	public static void initByTrackers(String trackerServers) throws IOException, MyException {
		synchronized (INITED) {
			if ( Boolean.FALSE.equals(INITED) ) {
				List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
				String spr1 = ",";
				String spr2 = ":";
				String[] arr1 = trackerServers.trim().split(spr1);
				for (String addrStr : arr1) {
					String[] arr2 = addrStr.trim().split(spr2);
					String host = arr2[0].trim();
					int port = Integer.parseInt(arr2[1].trim());
					list.add(new InetSocketAddress(host, port));
				}
				InetSocketAddress[] trackerAddresses = list.toArray(new InetSocketAddress[list.size()]);
				initByTrackers(trackerAddresses);
				initSocketPool();
				INITED = Boolean.TRUE;
			}
		}
	}

	public static void initByTrackers(InetSocketAddress[] trackerAddresses) throws IOException, MyException {
		synchronized (INITED) {
			if ( Boolean.FALSE.equals(INITED) ) {
				g_tracker_group = new TrackerGroup(trackerAddresses);
				initSocketPool();
				INITED = Boolean.TRUE;
			}
		}
	}

	/**
	 * construct Socket object
	 *
	 * @param ip_addr
	 *            ip address or hostname
	 * @param port
	 *            port number
	 * @return connected Socket object
	 * @throws Exception
	 */
	public static Socket getSocket(String ip_addr, int port) throws Exception {
		InetSocketAddress address = new InetSocketAddress(ip_addr, port);
		return ClientGlobal.SOCKET_POOL.borrowObject(address);
	}

	/**
	 * construct Socket object
	 *
	 * @param addr
	 *            InetSocketAddress object, including ip address and port
	 * @return connected Socket object
	 * @throws Exception
	 */
	public static Socket getSocket(InetSocketAddress addr) throws Exception {
		return ClientGlobal.SOCKET_POOL.borrowObject(addr);
	}

	public static int getG_connect_timeout() {
		return g_connect_timeout;
	}

	public static void setG_connect_timeout(int connect_timeout) {
		ClientGlobal.g_connect_timeout = connect_timeout;
	}

	public static int getG_network_timeout() {
		return g_network_timeout;
	}

	public static void setG_network_timeout(int network_timeout) {
		ClientGlobal.g_network_timeout = network_timeout;
	}

	public static String getG_charset() {
		return g_charset;
	}

	public static void setG_charset(String charset) {
		ClientGlobal.g_charset = charset;
	}

	public static int getG_tracker_http_port() {
		return g_tracker_http_port;
	}

	public static void setG_tracker_http_port(int tracker_http_port) {
		ClientGlobal.g_tracker_http_port = tracker_http_port;
	}

	public static boolean getG_anti_steal_token() {
		return g_anti_steal_token;
	}

	public static boolean isG_anti_steal_token() {
		return g_anti_steal_token;
	}

	public static void setG_anti_steal_token(boolean anti_steal_token) {
		ClientGlobal.g_anti_steal_token = anti_steal_token;
	}

	public static String getG_secret_key() {
		return g_secret_key;
	}

	public static void setG_secret_key(String secret_key) {
		ClientGlobal.g_secret_key = secret_key;
	}

	public static TrackerGroup getG_tracker_group() {
		return g_tracker_group;
	}

	public static void setG_tracker_group(TrackerGroup tracker_group) {
		ClientGlobal.g_tracker_group = tracker_group;
	}

	public static String configInfo() {
		String trackerServers = "";
		if (g_tracker_group != null) {
			InetSocketAddress[] trackerAddresses = g_tracker_group.tracker_servers;
			for (InetSocketAddress inetSocketAddress : trackerAddresses) {
				if (trackerServers.length() > 0)
					trackerServers += ",";
				trackerServers += inetSocketAddress.toString().substring(1);
			}
		}
		return "{" + "\n  g_connect_timeout(ms) = " + g_connect_timeout + "\n  g_network_timeout(ms) = "
				+ g_network_timeout + "\n  g_charset = " + g_charset + "\n  g_anti_steal_token = " + g_anti_steal_token
				+ "\n  g_secret_key = " + g_secret_key + "\n  g_tracker_http_port = " + g_tracker_http_port
				+ "\n  trackerServers = " + trackerServers + "\n}";
	}

}
