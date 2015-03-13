package com.lingo.mglory.resource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ResourcePool{
	 //前置类
	 public static final String FRONT_RESOURCE="front";
	 //执行类
	 public static final String EXEC_RESOURCE="exec";
	 //存储备份类
	 public static final String BAK_EXEC_RESOURCE="bakExec";
	 private static ResourcePool resourcePool;
	 //注册的资源
	 public ConcurrentMap<String,String> resourceMap=new ConcurrentHashMap<String,String>();
	 //前置类资源
	 public ConcurrentMap<String,String> frontResourceMap=new ConcurrentHashMap<String,String>();
	 //执行类资源
	 public ConcurrentMap<String,String> execResourceMap=new ConcurrentHashMap<String,String>();
	 //执行类备份资源
	 public ConcurrentMap<String,String> bakExecResourceMap=new ConcurrentHashMap<String,String>();
	 //注册资源上的服务信息
	 public ConcurrentMap<String,String> serviceMap=new ConcurrentHashMap<String,String>();
	 //注册资源的系统信息
	 public ConcurrentMap<String,SystemResource> resourceMonitorMap=new ConcurrentHashMap<String,SystemResource>();
	 //资源报告时间信息
	 public ConcurrentMap<String,Long> resourceMonitorTimeMap=new ConcurrentHashMap<String,Long>();
	 private String masterGroup;
	 private String slaveGroup;
	 private ResourcePool(){
			
	 }
	 public synchronized static ResourcePool getInstance()
	 {
			if (resourcePool==null)
			{
				resourcePool=new ResourcePool();
			}
			return resourcePool;
	 }
	 public String getMasterGroup()
	 {
		 return masterGroup;
	 }
	 public void setMasterGroup(String masterGroup)
	 {
		 this.masterGroup=masterGroup;
	 }
	 public String getSlaveGroup()
	 {
		 return slaveGroup;
	 }
	 public void setSlaveGroup(String slaveGroup)
	 {
		 this.slaveGroup=slaveGroup;
	 }
}
