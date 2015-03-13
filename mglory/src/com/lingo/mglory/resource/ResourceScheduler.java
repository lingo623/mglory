package com.lingo.mglory.resource;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryOut;

public class ResourceScheduler{
	private static Log log = LogFactory.getLog(ResourceScheduler.class);
	/**
	  * 获取空闲的注册资源
	  * @param taskId
	  * @param inMap
	  * @return
	  * @throws Exception
	  */
	 public MgloryOut getIdleResource(String taskId,Map<String,Object> inMap) throws Exception
	 {
		 MgloryOut out =new MgloryOut();
		 List<String> resourceList=new ArrayList<String>();
		 int num=Integer.parseInt(inMap.get("num").toString());
		 for (int i=0;i<num;i++)
		 {
			 resourceList.add(getIdleResource());
		 }
		 Map<String,Object> outMap=new HashMap<String,Object>();
		 outMap.put("resourceList", resourceList);
		 out.setMap(outMap);
		 return out;
	 }
	 /**
	  * 获取空闲的注册资源
	  * @return String
	  * @throws RemoteException
	  */
	 public String getIdleResource() throws Exception
	 {
		 ResourceManager resourceManager=new ResourceManager();
		 String resource="";
		 ConcurrentMap<String,String> resourceMap=resourceManager.getResource();
		 final ConcurrentMap<String,String> execResourceMap=resourceManager.getExecResource();
		 final ConcurrentMap<String,SystemResource> resourceMonitorMap=resourceManager.getResourceMonitor();
		 List<String> idleResourceList=new ArrayList<String>();
		 if (resourceMap.isEmpty())
		 {
			 throw new Exception("没有可用资源!");
		 }
		 final long t=System.currentTimeMillis();
		 ExecutorService executor=Executors.newFixedThreadPool(1);
		 FutureTask<List<String>> futureTask = new FutureTask<List<String> >(
					new Callable<List<String> >() {
						@Override
						public List<String>  call() throws Exception {
							List<String> resourceList=new ArrayList<String>();
							while(resourceList.isEmpty() && !resourceMonitorMap.isEmpty() 
								 && !execResourceMap.isEmpty() && (System.currentTimeMillis()-t)<12000)
							{
								log.info("获取空闲资源");
//								for (String key:resourceMonitorMap.keySet())
//								{
//									if (resourceMonitorMap.get(key).getFreeMemory()>1024*64
//									 && resourceMonitorMap.get(key).getCpuRatio()<0.95
//									 && resourceMonitorMap.get(key).getIoRatio()<0.70
//									 && resourceMonitorMap.get(key).getNetRatio()<0.90
//									 && execResourceMap.containsKey(key))
//									{
//										resourceList.add(key);
//									}
//								}
								for (String key:execResourceMap.keySet())
								{
									String value=execResourceMap.get(key).toString();
									if (resourceMonitorMap.get(value).getFreeMemory()>1024*64
									 && resourceMonitorMap.get(value).getCpuRatio()<0.95
									 && resourceMonitorMap.get(value).getIoRatio()<0.70
									 && resourceMonitorMap.get(value).getNetRatio()<0.90)
									{
										resourceList.add(value);
									}
								}
								if (!resourceList.isEmpty())
								{
									break;
								}
								Thread.sleep(3000);
							}
							return resourceList;
						}
		 });
		 executor.submit(futureTask);
		 try {
			idleResourceList=futureTask.get();
		 } catch (Exception e) {
			log.error("忽略异常====>"+e);
		 }
		 executor.shutdown();
		 if (idleResourceList.isEmpty())
		 {
			 Random r = new Random();
			 int n=r.nextInt(execResourceMap.size());
			 resource=execResourceMap.keySet().toArray()[n].toString();
		 }
		 else
		 {
			 Random r = new Random();
			 int n=r.nextInt(idleResourceList.size());
			 resource=idleResourceList.get(n);
		 }
		 return resource;
	 }
	 /**
	  * 获取primary
	  * @param taskId
	  * @param inMap
	  * @return
	  * @throws Exception
	  */
	 public MgloryOut getPrimary(String taskId,Map<String,Object> inMap)throws Exception
	 { 
		MgloryOut out =new MgloryOut();
		Map<String,Object> outMap=new HashMap<String,Object>();
	    outMap.put("primary",ElectManager.getInstance().getPrimary());
	    out.setMap(outMap);
		return out;
	 }
	 /**
	  * 同步primary
	  * @param taskId
	  * @param inMap
	  * @return
	  * @throws Exception
	  */
	 public MgloryOut synPrimary(String taskId,Map<String,Object> inMap)throws Exception
	 { 
		String primarySyn=inMap.get("primary").toString();
		String elector=inMap.get("elector").toString();
		long primaryLeaseSyn=Long.parseLong(inMap.get("primaryLease").toString());
		long primaryLease=ElectManager.getInstance().getPrimaryLease();
		if (primaryLeaseSyn>primaryLease)
		{
			ElectManager.getInstance().setPrimary(primarySyn);
			ElectManager.getInstance().setPrimaryLease(primaryLease);
			ElectManager.getInstance().setElector(elector);
			//资源为primary
			NodeManager.getInstance().setPrimaryFlag(true);
		}
		MgloryOut out =new MgloryOut();
		Map<String,Object> outMap=new HashMap<String,Object>();
	    outMap.put("elector",ElectManager.getInstance().getElector());
	    out.setMap(outMap);
		return out;
	 }
}
