package com.lingo.mglory.route;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.resource.ResourceHelper;
import com.lingo.mglory.transport.client.Client;
import com.lingo.mglory.util.Utils;

public class MgloryHashHelper {
	public static String getHashResource(String masterGroup,String slaveGroup,String key) throws Exception
	{
		Map<String,String> execResourceMap=getExecResourceMap(masterGroup,slaveGroup);
		return getResource(key, getMgloryHash(execResourceMap),execResourceMap,masterGroup,slaveGroup);
	}
	public static String getHashResource(String key,MgloryHash<Node<String>> mgloryHash,String masterGroup,String slaveGroup) throws Exception
	{
		Map<String,String> execResourceMap=getExecResourceMap(masterGroup,slaveGroup);
		return getResource(key,mgloryHash,execResourceMap,masterGroup,slaveGroup);
	}
	public static MgloryHash<Node<String>> getMgloryHash(Map<String,String> execResourceMap)
	{
		List<Node<String>> nodes=new ArrayList<Node<String>>();
		for (String resource:execResourceMap.keySet())
		{
			Node<String> node=new Node<String>(resource);
			nodes.add(node);
		}
		MgloryHash<Node<String>> mgloryHash=new MgloryHash<Node<String>>(1000,nodes);
		return mgloryHash;
	}
	public static String getResource(String key,MgloryHash<Node<String>> mgloryHash,Map<String,String> execResourceMap,
									 String masterGroup,String slaveGroup)throws Exception
	{
		if (key==null || key.equals(""))
		{
			throw new Exception("key不能为空");
		}
		int point=key.lastIndexOf("__");
		if (point!=-1)
		{
			key=key.substring(0,point);
		}
		String hashResource=mgloryHash.get(key).getResource();
		String resource=execResourceMap.get(hashResource);
		if (resource==null)
		{
			Map<String,String> bakExecResourceMap=getBakExecResource(masterGroup,slaveGroup);
			throw new Exception("资源："+hashResource+"和"+bakExecResourceMap.get(hashResource)+"都处于不可用状态，请检查！");
		}
		return resource;
	}
	public static Map<String,String> getExecResourceMap(String masterGroup,String slaveGroup) throws Exception
	{
		String resource=ResourceHelper.getInstance().getFrontResource(masterGroup, slaveGroup);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		MgloryIn in=new MgloryIn();
		in.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.getExecResource");
		MgloryOut out=Client.getInstance().call(ip, port, in);
		Map<String,Object> execResourceMap=out.getMap();
		return Utils.transToStrMap(execResourceMap);
	}
	public static Map<String,String> getBakExecResource(String masterGroup,String slaveGroup) throws Exception
	{
		String resource=ResourceHelper.getInstance().getFrontResource(masterGroup, slaveGroup);
		String ip=resource.split(":")[0];
		int port=Integer.parseInt(resource.split(":")[1]);
		MgloryIn in=new MgloryIn();
		in.setInvokeMethod("com.lingo.mglory.resource.ResourceManager.getBakExecResource");
		MgloryOut out=Client.getInstance().call(ip, port, in);
		Map<String,Object> bakExecResource=out.getMap();
		return Utils.transToStrMap(bakExecResource);
	}
}
