/*******************************************************************************
 * @Description:节点管理器
 * @author sunxuhang
 * @revision 1.0
 * @date 2013-12-17
 *******************************************************************************/
package com.lingo.mglory.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.server.MgloryGroupServer;
import com.lingo.mglory.server.MgloryWorkerServer;
import com.lingo.mglory.transport.client.Client;
import com.lingo.mglory.transport.server.Server;
import com.lingo.mglory.util.PropertiesUtil;

public class NodeManager {
	private static Log log = LogFactory.getLog(NodeManager.class);
	private Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
	private static NodeManager nodeManager;
	private String masterGroup;
	private String slaveGroup;
	private boolean isGroup;
	private boolean synResourceFlag;
	private boolean cleanUpFlag;
	private boolean electFlag=false;
	private boolean primaryFlag=true;
	private NodeManager()
	{
		
	}
	public synchronized static NodeManager getInstance()
	{
		if (nodeManager==null)
		{
			nodeManager=new NodeManager();
		}
		return nodeManager;
	}
	public void startHeart(boolean isGroup)
	{
		String masterIp=config.getProperty("masterIp");
		String masterPort=config.getProperty("masterPort");
		String slaveIp=config.getProperty("slaveIp");
		String slavePort=config.getProperty("slavePort");
		masterGroup=masterIp+":"+masterPort;
		slaveGroup=slaveIp+":"+slavePort;
		this.isGroup=isGroup;
		if (isGroup)
		{
			String localResource=MgloryGroupServer.getInstance().getIp()+":"+MgloryGroupServer.getInstance().getPort();
			//启动时只有主资源管理器启动选举器
			if (localResource.equals(masterGroup))
			{
				electFlag=true;
			}
			//选举primary
			electPrimary();
		}
		else
		{
			//执行服务报告资源信息
			reportResource();
		}
		String resource=MgloryWorkerServer.getInstance().getResource();
		registerShutdownHook(resource);
	}
	//选举primary
	private void electPrimary()
	{
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
				new Callable<Map<String,Object>>() {
					@Override
					public Map<String,Object> call() throws Exception {
						while(true)
						{
							if (electFlag)
							{
								try
								{
									ElectManager.getInstance().electPrimary();
								}
								catch(Exception e)
								{
									log.error("elect primary error===>"+e);
								}
							}
							log.info("primary====>"+ElectManager.getInstance().getPrimary());
							String localResource=MgloryGroupServer.getInstance().getIp()+":"+MgloryGroupServer.getInstance().getPort();
							setElectFlag(localResource.equals(ElectManager.getInstance().getElector()));
							Thread.sleep(5000);
						}
					}
		});
		executor.submit(futureTask);
		executor.shutdown();
	}
	private void synResuorceToFront()
	{
		 synResourceFlag=true;
		 ExecutorService executor=Executors.newFixedThreadPool(1);
		 FutureTask<MgloryOut> futureTask = new FutureTask<MgloryOut>(
						new Callable<MgloryOut>() {
							@Override
							public MgloryOut call() throws Exception {
								while(primaryFlag)
								{
									try
									{
										Map<String,String> frontResourceMap=ResourcePool.getInstance().frontResourceMap;
										frontResourceMap.put(masterGroup, masterGroup);
										frontResourceMap.put(slaveGroup, slaveGroup);
										frontResourceMap.remove(ElectManager.getInstance().getPrimary());
										for (String resource:frontResourceMap.keySet())
										{
											String ip=resource.split(":")[0];
											int port=Integer.parseInt(resource.split(":")[1]);
											Map<String,Object> map=new HashMap<String,Object>();
											map.put("resourceMap",ResourcePool.getInstance().resourceMap);
											map.put("execResourceMap",ResourcePool.getInstance().execResourceMap);
											map.put("bakExecResourceMap",ResourcePool.getInstance().bakExecResourceMap);
											map.put("resourceMonitorMap",ResourcePool.getInstance().resourceMonitorMap);
											map.put("resourceMonitorTimeMap",ResourcePool.getInstance().resourceMonitorTimeMap);
											map.put("primary",ElectManager.getInstance().getPrimary());
											map.put("primaryLease",ElectManager.getInstance().getPrimaryLease());
											map.put("elector",ElectManager.getInstance().getElector());
											MgloryIn mgloryIn=new MgloryIn();
											mgloryIn.setMap(map);
											mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.synResuorceToFront");
											Client.getInstance().call(ip, port, mgloryIn);
										}
									}
									catch(Exception e)
									{
										log.error("忽略异常====>"+e);
									}
									Thread.sleep(5000);
								}
								synResourceFlag=false;
								return null;
							}
							
			 });
			 executor.submit(futureTask);
			 executor.shutdown();
	 }
	//清除僵死资源
	private void cleanUpDeadResource()
	{
		cleanUpFlag=true;
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
				new Callable<Map<String,Object>>() {
					@Override
					public Map<String,Object> call() throws Exception {
						while(primaryFlag)
						{
							try
							{
								log.info("清除僵死资源");
								ResourceManager resourceManager=new ResourceManager();
								ConcurrentMap<String,Long> resourceMonitorTimeMap=resourceManager.getResourceMonitorTime();
								for (String key:resourceMonitorTimeMap.keySet())
								{
									long t=System.currentTimeMillis();
									long monitorTime=resourceMonitorTimeMap.get(key);
									if (monitorTime-t>10000)
									{
										resourceManager.removeResource(key);
									}
								}
							}
							catch(Exception e)
							{
								log.error("cleanup resource error===>"+e);
							}
							Thread.sleep(3000);
						}
						cleanUpFlag=false;
						return null;
					}
		});
		executor.submit(futureTask);
		executor.shutdown();
	}
	//心跳程序，同步资源给primary
	private void reportResource()
	{
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
				new Callable<Map<String,Object>>() {
					@Override
					public Map<String,Object> call() throws Exception {
						while(true)
						{
							log.info("报告资源");
							String primary=ElectManager.getInstance().getPrimary();
							if (primary.equals(""))
							{
								primary=masterGroup;
							}
							String resource=MgloryWorkerServer.getInstance().getResource();
							if (!primary.equals(resource))
							{
								Map<String,Object> inMap=new HashMap<String,Object>();
								inMap.put("resource",resource);
								inMap.put("resourceType", MgloryWorkerServer.getInstance().getResourceType());
								inMap.put("systemResource",ResourceUsage.getResUsage());
								MgloryIn mgloryIn=new MgloryIn();
								mgloryIn.setMap(inMap);
								mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.synResuorce");
								try
								{
									String ip=primary.split(":")[0];
									int port=Integer.parseInt(primary.split(":")[1]);
									MgloryOut out=Client.getInstance().call(ip,port, mgloryIn);
									ElectManager.getInstance().setPrimary(out.getMap().get("primary").toString());
								}
								catch(Exception e)
								{
									log.error("同步资源到primary服务失败===>"+e);
									MgloryIn in=new MgloryIn();
									mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceScheduler.getPrimary");
									try
									{
										String ip=masterGroup.split(":")[0];
										int port=Integer.parseInt(masterGroup.split(":")[1]);
										MgloryOut out=Client.getInstance().call(ip,port,in);
										ElectManager.getInstance().setPrimary(out.getMap().get("primary").toString());
									}
									catch(Exception em)
									{
										log.error("获取primary失败===>"+em);
										if (!slaveGroup.equals(masterGroup))
										try
										{
											String ip=slaveGroup.split(":")[0];
											int port=Integer.parseInt(slaveGroup.split(":")[1]);
											MgloryOut out=Client.getInstance().call(ip,port,in);
											ElectManager.getInstance().setPrimary(out.getMap().get("primary").toString());
										}
										catch(Exception es)
										{
											log.error("获取primary失败===>"+es);
										}
									}
							}
							
							}
							Thread.sleep(3000);
						}
					}
		});
		executor.submit(futureTask);
		executor.shutdown();
	}
	public void setElectFlag(boolean electFlag)
	{
		this.electFlag=electFlag;
	}
	public void setPrimaryFlag(boolean primaryFlag)
	{
		if (primaryFlag && !synResourceFlag)
		{
			synResuorceToFront();
		}
		if (primaryFlag && !cleanUpFlag)
		{
			cleanUpDeadResource();
		}
		this.primaryFlag=primaryFlag;
	}
	/**
	 * 钩子方法
	 * @param resource
	 */
	private void registerShutdownHook(final String resource){
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				if (!isGroup)
				{
					String primary=ElectManager.getInstance().getPrimary();
					if (primary.equals(""))
					{
						primary=masterGroup;
					}
					if (!primary.equals(resource))
					{
						Map<String,Object> inMap=new HashMap<String,Object>();
						inMap.put("resource", resource);
						MgloryIn mgloryIn=new MgloryIn();
						mgloryIn.setMap(inMap);
						mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.removeResource");
						try
						{
							String ip=primary.split(":")[0];
							int port=Integer.parseInt(primary.split(":")[1]);
							Client.getInstance().call(ip, port, mgloryIn);
						}
						catch(Exception e)
						{
							log.error("清除资源失败===>"+e);
						}
					}
				}
				Server.shutdown();
			}
		});
	}
  
}
