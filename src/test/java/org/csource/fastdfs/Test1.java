package org.csource.fastdfs;

import org.apache.log4j.PropertyConfigurator;
import org.csource.common.NameValuePair;

public class Test1 {
	public static void main(String args[]) {
		try {
			PropertyConfigurator.configure("log4j.properties");
			ClientGlobal.initByProperties("fastdfs-client.properties");
			System.out.println("network_timeout=" + ClientGlobal.g_network_timeout + "ms");
			System.out.println("charset=" + ClientGlobal.g_charset);
			ClientGlobal.showPoolStatus();
//			TrackerGroup tg = new TrackerGroup(new InetSocketAddress[] { new InetSocketAddress("10.30.202.66", 22122) });
			TrackerClient tc = new TrackerClient();
			ClientGlobal.showPoolStatus();
			
			TrackerServer ts = tc.getConnection();
			ClientGlobal.showPoolStatus();
			if (ts == null) {
				System.out.println("getConnection return null");
				return;
			}

			StorageServer ss = tc.getStoreStorage(ts);
			ClientGlobal.showPoolStatus();
			if (ss == null) {
				System.out.println("getStoreStorage return null");
			}

			StorageClient1 sc1 = new StorageClient1(ts, ss);
			ClientGlobal.showPoolStatus();

			NameValuePair[] meta_list = null; // new NameValuePair[0];
			String item;
			String fileid;
//			if (System.getProperty("os.name").equalsIgnoreCase("windows")) {
//				item = "c:/windows/system32/notepad.exe";
//				fileid = sc1.upload_file1(item, "exe", meta_list);
//			} else {
//				item = "/etc/hosts";
//				fileid = sc1.upload_file1(item, "", meta_list);
//			}

			item = "C:/windows/win.ini";
			fileid = sc1.upload_file1(item, "ini", meta_list);
			ClientGlobal.showPoolStatus();
			System.out.println("Upload local file " + item + " ok, fileid=" + fileid);
			ss.close();
			ts.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			ClientGlobal.showPoolStatus();
		}
	}
}
