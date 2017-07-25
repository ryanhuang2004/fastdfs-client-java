package org.csource.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 基于Apache Common Pool 2的Socket连接池工厂
 * @author Ryan Huang
 * @version 1.0
 *
 */
public class SocketPoolFactory implements KeyedPooledObjectFactory<InetSocketAddress, Socket>{
	
	private static final Logger logger = LoggerFactory.getLogger(SocketPoolFactory.class);

	@Override
	public void activateObject(InetSocketAddress address, PooledObject<Socket> pooledObject) throws Exception {
		logger.trace("activating socket object with InetSocketAddress : [{}]", address);
	}

	@Override
	public void destroyObject(InetSocketAddress address, PooledObject<Socket> pooledObject) throws Exception {
		logger.trace("destroying Socket Object with address [{}:{}]", pooledObject.getObject().getInetAddress(), pooledObject.getObject().getPort());
		byte[] header = ProtoCommon.packHeader(ProtoCommon.FDFS_PROTO_CMD_QUIT, 0, (byte) 0);
	    pooledObject.getObject().getOutputStream().write(header);
	    pooledObject.getObject().getOutputStream().flush();
		pooledObject.getObject().close();
	}

	@Override
	public PooledObject<Socket> makeObject(InetSocketAddress address) throws Exception {
		Socket sock = new Socket();
	    sock.setReuseAddress(true);
	    sock.setKeepAlive(true);
//	    sock.setSoTimeout(ClientGlobal.g_network_timeout);
	    sock.connect(address, ClientGlobal.g_connect_timeout);
		return new DefaultPooledObject<Socket>(sock);
	}

	@Override
	public void passivateObject(InetSocketAddress address, PooledObject<Socket> pooledObject) throws Exception {
		logger.trace("Passivating Socket Object with InetSocketAddress : [{}]", address);
	}

	@Override
	public boolean validateObject(InetSocketAddress address, PooledObject<Socket> pooledObject) {
		logger.trace("validating socket object with InetSocketAddress [{}:{}]", pooledObject.getObject().getInetAddress(), pooledObject.getObject().getPort());
		Socket sock = pooledObject.getObject();
		InetSocketAddress actualAddress = new InetSocketAddress(sock.getInetAddress(), sock.getPort());
		if ( !actualAddress.equals(address) ) {
			logger.warn("Validating Socket Object failed since the actual InetSocketAddress for the socket"
					+ " object is : [{}], and the InetSocketAddress user required is : [{}]", actualAddress, address);
			return false;
		}
		if ( !sock.isConnected() || sock.isInputShutdown() || sock.isOutputShutdown() ) {
			logger.debug("Validating Socket Object failed since the connection is down or the input stream is down or the outputsteam is down");
			return false;
		}
		try {
			if ( !ProtoCommon.activeTest(sock) ) {
				logger.debug("Validating Socket Object failed since its not pass the activeTest");
				return false;
			}
		} catch (IOException e) {
			logger.debug("Validating Socket Object failed since activeTest throws IOException", e);
			return false;
		}
		return true;
	}

}