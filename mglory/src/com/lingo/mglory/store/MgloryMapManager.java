package com.lingo.mglory.store;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.transport.client.Client;

public class MgloryMapManager {
	private static Log log = LogFactory.getLog(MgloryMapManager.class);
	public MgloryOut put(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		String key=inMap.get("key").toString();
		Object value=inMap.get("value");
		MgloryMapStore.getInstance().mgloryMap.put(key,value);
		if (MgloryMapStore.getInstance().synFlag)
		{
			//加入待同步队列
			Map<String,Map<String,Object>> operMap=new HashMap<String,Map<String,Object>>();
			operMap.put("put",inMap);
			MgloryMapStore.getInstance().operQueue.add(operMap);
		}
		return out;
	}
	@SuppressWarnings("unchecked")
	public MgloryOut batchPut(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		List<Map<String,Object>> updateList=(List<Map<String,Object>>)inMap.get("list");
		for (Map<String,Object> map:updateList)
		{
			String key=map.get("key").toString();
			Object value=map.get("value");
			MgloryMapStore.getInstance().mgloryMap.put(key,value);
		}
		if (MgloryMapStore.getInstance().synFlag)
		{
			//加入待同步队列
			Map<String,Map<String,Object>> operMap=new HashMap<String,Map<String,Object>>();
			operMap.put("batchPut",inMap);
			MgloryMapStore.getInstance().operQueue.add(operMap);
		}
		return out;
	}
	public MgloryOut get(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		String key=inMap.get("key").toString();
		Map<String,Object> resultMap=new HashMap<String,Object>();
		resultMap.put(key,MgloryMapStore.getInstance().mgloryMap.get(key));
		out.setMap(resultMap);
		return out;
	}
	public MgloryOut getAllByRootKey(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		String rootKey=inMap.get("rootKey").toString();
		Map<String,Object> resultMap=new HashMap<String,Object>();
		for (String key:MgloryMapStore.getInstance().mgloryMap.keySet())
		{
			if (key.startsWith(rootKey+"__"))
			{
				resultMap.put(key,MgloryMapStore.getInstance().mgloryMap.get(key));
			}
		}
		out.setMap(resultMap);
		return out;
	}
	public MgloryOut getAll(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		out.setMap(MgloryMapStore.getInstance().mgloryMap);
		return out;
	}
	public MgloryOut remove(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		String key=inMap.get("key").toString();
		MgloryMapStore.getInstance().mgloryMap.remove(key);
		if (MgloryMapStore.getInstance().synFlag)
		{
			//加入待同步队列
			Map<String,Map<String,Object>> operMap=new HashMap<String,Map<String,Object>>();
			operMap.put("remove", inMap);
			MgloryMapStore.getInstance().operQueue.add(operMap);
		}
		return out;
	}
	public MgloryOut clear(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		MgloryMapStore.getInstance().mgloryMap.clear();
		if (MgloryMapStore.getInstance().synFlag)
		{
			//加入待同步队列
			Map<String,Map<String,Object>> operMap=new HashMap<String,Map<String,Object>>();
			operMap.put("clear", inMap);
			MgloryMapStore.getInstance().operQueue.add(operMap);
		}
		return out;
	}
	public MgloryOut getKeys(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		Map<String,Object> keyMap=new HashMap<String,Object>();
		for (String key:MgloryMapStore.getInstance().mgloryMap.keySet())
		{
			keyMap.put(key,"");
		}
		out.setMap(keyMap);
		return out;
	}
	@SuppressWarnings("unchecked")
	public MgloryOut syn(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		List<Map<String,Map<String,Object>>> list=(List<Map<String,Map<String,Object>>>)inMap.get("list");
		for (Map<String,Map<String,Object>> operMap:list)
		{
			String operType=operMap.keySet().toArray()[0].toString();
			Method method = MgloryMapStore.getInstance().methodMap.get(operType);
			try {
				method.invoke(this,operMap.get(operType));
			} catch (Exception e) {
				log.error("异常忽略不计:"+e);
			}
		}
		return out;
	}
	public MgloryOut bakToPrimary(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		String resource=inMap.get("resource").toString();
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		MgloryIn in=new MgloryIn();
		inMap=new HashMap<String,Object>();
		inMap.put("mgloryMap", MgloryMapStore.getInstance().mgloryMap);
		in.setMap(inMap);
		in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.receiveFromBak");
		Client.getInstance().call(ip, port, in);
		return out;
	}
	@SuppressWarnings("unchecked")
	public MgloryOut receiveFromBak(String taskId,Map<String,Object> inMap)
	{
		MgloryOut out=new MgloryOut();
		MgloryMapStore.getInstance().mgloryMap=(ConcurrentMap<String,Object>)inMap.get("mgloryMap");
		return out;
	}
}
