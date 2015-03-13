package com.lingo.mglory.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.route.MgloryHashHelper;
import com.lingo.mglory.transport.client.Client;

public class MgloryMap{
	private static Log log = LogFactory.getLog(MgloryMap.class);
	private String masterGroup;//中心服务（资源管理器）主机地址 如：114.215.178.115:1099
	private String slaveGroup;//中心服务（资源管理器）备机地址
    public MgloryMap(String masterGroup,String slaveGroup)
    {
    	this.masterGroup=masterGroup;
		this.slaveGroup=slaveGroup;
	}
    /**
	 * 写入
	 * @param key
	 * @param value
	 * @throws Exception
	 */
	public void put(String key,Object value) throws Exception
	{
		String resource=MgloryHashHelper.getHashResource(masterGroup, slaveGroup,key);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		Map<String,Object> inMap=new HashMap<String,Object>();
		inMap.put("key",key);
		inMap.put("value",value);
		MgloryIn in=new MgloryIn();
		in.setMap(inMap);
		in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.put");
		Client.getInstance().call(ip, port, in);
	}
	/**
	 * 批量写入
	 * @param list
	 * @throws Exception
	 */
	public void batchPut(List<Map<String,Object>> memorylist) throws Exception
	{
		Map<String,List<Map<String,Object>>> resourceUpdateListMap=new HashMap<String,List<Map<String,Object>>>();
		for (Map<String,Object> map:memorylist)
		{
			if (!map.containsKey("key") || !map.containsKey("value"))
			{
				log.error("key缺失");
				return;
			}
			else
			{
				String key=map.get("key").toString();
				String resource=MgloryHashHelper.getHashResource(masterGroup, slaveGroup,key);
				if (resourceUpdateListMap.containsKey(resource))
				{
					List<Map<String,Object>> updateList=resourceUpdateListMap.get(resource);
					updateList.add(map);
				}
				else
				{
					List<Map<String,Object>> updateList=new ArrayList<Map<String,Object>>();
					updateList.add(map);
					resourceUpdateListMap.put(resource,updateList);
				}
			}
		}
		if (!resourceUpdateListMap.isEmpty())
		{
			ExecutorService taskExecutor=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			List<FutureTask<MgloryOut>> futureTasks = new ArrayList<FutureTask<MgloryOut>>();
			for (String key:resourceUpdateListMap.keySet()) {
				final String resource=key;
				final List<Map<String,Object>> updateList=resourceUpdateListMap.get(resource);
				FutureTask<MgloryOut> futureTask = new FutureTask<MgloryOut>(
						new Callable<MgloryOut>() {
							@Override
							public MgloryOut call() throws Exception {
								Map<String,Object> inMap=new HashMap<String,Object>();
								inMap.put("list",updateList);
								MgloryIn in=new MgloryIn();
						        in.setMap(inMap);
								in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.batchPut");
								String ip=resource.split(":")[0];
								int port=Integer.parseInt(resource.split(":")[1]);
								return Client.getInstance().call(ip, port, in);
							}
						});

				futureTasks.add(futureTask);
				taskExecutor.submit(futureTask);
			}
			for (FutureTask<MgloryOut> futureTask : futureTasks) {
				try {
					futureTask.get();
				} catch (Exception e) {
					//throw e;
					log.error("error===>"+e);
				}
			}
			taskExecutor.shutdown();
		}
	}
	/**
	 * 读
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public Object get(String key) throws Exception
	{
		String resource=MgloryHashHelper.getHashResource(masterGroup, slaveGroup,key);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		Map<String,Object> inMap=new HashMap<String,Object>();
		inMap.put("key",key);
		MgloryIn in=new MgloryIn();
		in.setMap(inMap);
		in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.get");
		MgloryOut out=Client.getInstance().call(ip, port, in);
		Object result=out.getMap().get(key);
		return result;
		
	}
	/**
	 * 读一级key下的所有信息
	 * @param rootKey
	 * @return
	 * @throws Exception
	 */
	public MgloryOut getAllByRootKey(String rootKey) throws Exception
	{
		String resource=MgloryHashHelper.getHashResource(masterGroup, slaveGroup,rootKey);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		Map<String,Object> inMap=new HashMap<String,Object>();
		inMap.put("rootKey",rootKey);
		MgloryIn in=new MgloryIn();
		in.setMap(inMap);
		in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.getAllByRootKey");
		MgloryOut out=Client.getInstance().call(ip, port, in);
		return out;
	}
	/**
	 * 读所有数据
	 * @return
	 * @throws Exception
	 */
	public List<MgloryOut> getAll() throws Exception
	{
		List<MgloryOut> result=new ArrayList<MgloryOut>();
		Map<String,String> execResourceMap=MgloryHashHelper.getExecResourceMap(masterGroup,slaveGroup);
		for (String resource:execResourceMap.keySet())
		{
			try
			{
				String ip=resource.split(":")[0];
				int port=Integer.parseInt(resource.split(":")[1]);
				MgloryIn in=new MgloryIn();
				in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.getAll");
				MgloryOut out=Client.getInstance().call(ip, port, in);
				result.add(out);
			}
			catch(Exception e)
			{
				 log.error("error===>"+e);
				 throw new Exception("读是有数据失败:"+e);
			}			
		}
		return result;
	}
	/**
	 * 移除key
	 * @param key
	 */
	public void remove(String key)throws Exception
	{
		String resource=MgloryHashHelper.getHashResource(masterGroup, slaveGroup,key);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		Map<String,Object> inMap=new HashMap<String,Object>();
		inMap.put("key",key);
		MgloryIn in=new MgloryIn();
		in.setMap(inMap);
		in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.remove");
		Client.getInstance().call(ip, port, in);
	}
	/**
	 * 清空
	 * @throws Exception
	 */
	public void clear()throws Exception
	{
		Map<String,String> execResourceMap=MgloryHashHelper.getExecResourceMap(masterGroup,slaveGroup);
		for (String resource:execResourceMap.keySet())
		{
			try
			{
				String ip=resource.split(":")[0];
				int port=Integer.parseInt(resource.split(":")[1]);
				MgloryIn in=new MgloryIn();
				in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.clear");
				Client.getInstance().call(ip, port, in);
			}
			catch(Exception e)
			{
				 log.error("error===>"+e);
				 throw new Exception("清空失败:"+e);
			}			
		}
	}
}
