package com.lingo.mglory.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.transport.client.Client;

public class ResourceHelper {
	private static Log log = LogFactory.getLog(ResourceHelper.class);
	private static ResourceHelper helper;
	private ConcurrentMap<String,String> groupMap=new ConcurrentHashMap<String,String>();
	private ConcurrentMap<String,Map<String,Object>> groupFrontMap=new ConcurrentHashMap<String,Map<String,Object>>();
	private String primary="";
	private ResourceHelper(){
		init();
	}
	public synchronized static ResourceHelper getInstance()
	{
		if (helper==null)
		{
			helper=new ResourceHelper();
		}
		return helper;
	}
	/**
	 * 获取空闲资源
	 * @param masterGroup
	 * @param slaveGroup
	 * @param num
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public List<String> getIdleResource(String masterGroup,String slaveGroup,int num) throws Exception
	{
		List<String>resourceList=new ArrayList<String>();
		String managerResource=getFrontResource(masterGroup,slaveGroup);
		Map<String,Object> inMap=new HashMap<String,Object>();
		inMap.put("num",num);
		MgloryIn mgloryIn=new MgloryIn();
		mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceScheduler.getIdleResource");
		mgloryIn.setMap(inMap);
		String ip=managerResource.split(":")[0];
		int port=Integer.parseInt(managerResource.split(":")[1]);
		MgloryOut mgloryOut=Client.getInstance().call(ip, port, mgloryIn);
		resourceList=(List<String>)mgloryOut.getMap().get("resourceList");
		if (resourceList==null)
		{
			for (int i=0;i<5;i++)
			{
				try
				{
					String masterIp=masterGroup.split(":")[0];
					int masterPort=Integer.parseInt(masterGroup.split(":")[1]);
					MgloryOut out=Client.getInstance().call(masterIp, masterPort, mgloryIn);
					resourceList=(List<String>)out.getMap().get("resourceList");
					break;
				} catch(Exception e)
				{
					if (i==4)
					{
						String slaveIp=slaveGroup.split(":")[0];
						int slavePort=Integer.parseInt(slaveGroup.split(":")[1]);
						MgloryOut out=Client.getInstance().call(slaveIp, slavePort, mgloryIn);
						resourceList=(List<String>)out.getMap().get("resourceList");
					}
				}
			}
		}
		if (resourceList==null)
		{
			resourceList=new ArrayList<String>();
		}
		return resourceList;
	}
	/**
	 * 获取空闲资源
	 * @param masterGroup
	 * @param slaveGroup
	 * @return
	 * @throws Exception
	 */
	public String getIdleResource(String masterGroup,String slaveGroup) throws Exception
	{
		List<String> resourceList=new ArrayList<String>();
		resourceList=getIdleResource(masterGroup, slaveGroup,1);
		if (resourceList.isEmpty())
		{
			throw new Exception("没有获取到空闲资源");
		}
		String resource=resourceList.get(0);
		return resource;
	}
	private void init()
	{
		ExecutorService executor=Executors.newFixedThreadPool(1);
		FutureTask<Map<String,Object>> futureTask = new FutureTask<Map<String,Object>>(
			new Callable<Map<String,Object>>() {
							@SuppressWarnings("unchecked")
							@Override
							public Map<String,Object> call() throws Exception {
								while(true)
								{
									for (String group:groupMap.keySet())
									{
										try
										{
											String ip=group.split(":")[0];
											int port=Integer.parseInt(group.split(":")[1]);
											MgloryIn mgloryIn=new MgloryIn();
											mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.getFrontResource");
											MgloryOut mgloryOut=Client.getInstance().call(ip, port, mgloryIn);
											primary= mgloryOut.getMap().get("primary").toString();
											groupFrontMap.put(group,(Map<String,Object>)mgloryOut.getMap().get("front"));
										}
										catch(Exception e)
										{
											if (groupFrontMap.size()>1)
											{
												groupFrontMap.remove(group);
											}
											log.error("忽略异常："+e);
										}
										
									}
									Thread.sleep(60000);
								}
							}
							
			 });
			 executor.submit(futureTask);
			 executor.shutdown();
	}
	public  String getFrontResource(String masterGroup,String slaveGroup)
	{
		if (!groupMap.containsKey(masterGroup))
		{
			groupMap.put(masterGroup, masterGroup);
		}
		if (!groupMap.containsKey(slaveGroup))
		{
			groupMap.put(slaveGroup, slaveGroup);
		}
		String resource="";
		Map<String,Object> masterFrontMap=groupFrontMap.get(masterGroup);
		Map<String,Object> slaveFrontMap=groupFrontMap.get(slaveGroup);
		if (masterFrontMap!=null)
		{
			masterFrontMap.remove(primary);
			if (!masterFrontMap.isEmpty())
			{
				Random r = new Random();
				int n=r.nextInt(masterFrontMap.size());
				resource=masterFrontMap.keySet().toArray()[n].toString();
			}
		}
		else if(slaveFrontMap!=null && !masterGroup.equals(slaveGroup))
		{
			slaveFrontMap.remove(primary);
			if (!slaveFrontMap.isEmpty())
			{
				Random r = new Random();
				int n=r.nextInt(slaveFrontMap.size());
				resource=slaveFrontMap.keySet().toArray()[n].toString();
			}
		}
		if (resource.equals(""))
		{
			Random r = new Random();
			int n=r.nextInt(groupMap.size());
			resource = groupMap.keySet().toArray()[n].toString();
		}
		return resource;
	}
}
