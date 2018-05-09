package com.pl.index;

import org.apache.hadoop.conf.Configuration;

public class dfsConfig {

	public enum h_mode {  
		  HA, NORMAL,LOCAL  
	}
	
	public final static String LOG_FLAG 		= "*** indexWord :";
	public final static String LOCAL_PATH 		= "e:/tmp";
	public final static String LOCAL_FILE 		= "/Traffic.data";
	public final static String FS_DEFAULTFS		= "hdfs://namecluster1";
	public final static String DFS_PATH_ROOT	= "/test";
	public final static String DFS_PATH_IN 		= DFS_PATH_ROOT + "/in";
	public final static String DFS_FULL_PATH_IN = FS_DEFAULTFS + DFS_PATH_ROOT + "/in"; 
	public final static String DFS_PATH_OUT		= DFS_PATH_ROOT + "/out"; 
	public final static String DFS_FULL_PATH_OUT= FS_DEFAULTFS + dfsConfig.DFS_PATH_ROOT + "/out";
	
	public final static String RM_HOSTNAME		= "NameNode01";
	public final static String DFS_JAR 			= "target/mvn_mr_index-0.0.1-SNAPSHOT.jar";

	// 配置服务器
	static Configuration _conf = null;

	public static Configuration getConf() {
		return getConf(h_mode.LOCAL);
	}
	public static Configuration getConf(h_mode mrmd) {

		if (_conf == null) {
			// 配置服务器
			Configuration conf = new Configuration(); 
			
			conf.set("fs.defaultFS", dfsConfig.FS_DEFAULTFS);
			conf.set("dfs.replication", "2");
			conf.set("mapreduce.job.jar", dfsConfig.DFS_JAR);
			
			if (mrmd==h_mode.NORMAL) { 
				// RM非HA
				conf.set("yarn.resourcemanager.hostname", RM_HOSTNAME); 
				
			}else if (mrmd==h_mode.HA) {
				// HDFS-HA
				conf.set("dfs.nameservices", "namecluster1");
				conf.set("dfs.ha.namenodes.namecluster1", "nn1,nn2");
				conf.set("dfs.namenode.rpc-address.namecluster1.nn1", "NameNode01:9000");
				conf.set("dfs.namenode.rpc-address.namecluster1.nn2", "NameNode02:9000");
				conf.set("dfs.client.failover.proxy.provider.namecluster1",
						"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
	
				conf.set("mapreduce.framework.name", "yarn");
				conf.set("mapreduce.app-submission.cross-platform", "true"); 
				// RM-HA
				conf.set("yarn.resourcemanager.ha.enabled", "true");
				conf.set("yarn.resourcemanager.cluster-id", "yarncluster");
				conf.set("yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
				conf.set("yarn.resourcemanager.hostname.rm1", "DataNode01");
				conf.set("yarn.resourcemanager.hostname.rm2", "DataNode02"); 
			}
			
			_conf = conf;
		}

		return _conf;
	}

}
