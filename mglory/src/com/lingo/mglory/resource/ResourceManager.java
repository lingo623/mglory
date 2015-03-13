/*******************************************************************************
 * @Description:资源管理类
 * @author sunxuhang
 * @revision 1.0
 * @date 2013-12-17
 *******************************************************************************/
package com.lingo.mglory.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.transport.client.Client;
import com.lingo.mglory.util.Utils;
public class ResourceManager{
	/**
	  * 获取注册资源
	  * @return ConcurrentMap<String,String>
	  */
	 public ConcurrentMap<String,String> getResource()
	 {
		 return ResourcePool.getInstance().resourceMap;
	 }
	 /**
	  * 获取执行类资源
	  * @return ConcurrentMap<String,String>
	  */
	 public ConcurrentMap<String,String> getExecResource()
	 {
		 return ResourcePool.getInstance().execResourceMap;
	 }
	 /**
	  * 获取前置类资源
	  * @return ConcurrentMap<String,String>
	  */
	 public ConcurrentMap<String,String> getFrontResource()
	 {
		 return ResourcePool.getInstance().frontResourceMap;
	 }
	 /**
	  * 获取执行类备份资源
	  * @return ConcurrentMap<String,String>
	  */
	 public ConcurrentMap<String,String> getBakExecResource()
	 {
		 return ResourcePool.getInstance().bakExecResourceMap;
	 }
	 /**
	  * 获取resourceMonitor
	  * @return
	  */
	 public ConcurrentMap<String,SystemResource> getResourceMonitor()
	 {
		 return ResourcePool.getInstance().resourceMonitorMap;
	 }
	/**
	 * 注册资源
	 * @param taskId
	 * @param inMap resource：127.0.0.1:8888,resourceType:exec、front、bakExec
	 * @return
	 * @throws Exception
	 */
	 public MgloryOut addResource(String taskId,Map<String,Object> inMap) throws Exception
	 {
		 MgloryOut out =new MgloryOut();
		 String resource=inMap.get("resource").toString();
		 String resourceType=inMap.get("resourceType").toString();
		 //执行类资源
		 if (resourceType.equals(ResourcePool.EXEC_RESOURCE))
		 {
			 if (!ResourcePool.getInstance().execResourceMap.containsKey(resource))
			 {
				 ResourcePool.getInstance().execResourceMap.put(resource,resource);//value为在用资源
			 }
			 else if (!ResourcePool.getInstance().execResourceMap.get(resource).equals(resource))
			 {
				 //数据迁移(备迁往主)
				 String bakResource=ResourcePool.getInstance().execResourceMap.get(resource);
				 String ip=bakResource.split(":")[0];
				 int port=Integer.parseInt(bakResource.split(":")[1]);
				 MgloryIn in=new MgloryIn();
				 inMap=new HashMap<String,Object>();
				 inMap.put("resource",resource);
				 in.setMap(inMap);
				 in.setInvokeMethod("com.lingo.mglory.store.MgloryMapManager.bakToPrimary");
				 Client.getInstance().call(ip, port, in);
				 ResourcePool.getInstance().execResourceMap.put(resource,resource);
			 }
		 }
		 //前置类资源
		 else if (resourceType.equals(ResourcePool.FRONT_RESOURCE))
		 {
			 if (!ResourcePool.getInstance().frontResourceMap.containsKey(resource))
			 {
				 ResourcePool.getInstance().frontResourceMap.put(resource,resourceType);
			 }
		 }
		 //执行类备份资源
		 else
		 {
			 //resourceType即需要备份的资源 ,如127.0.0.1:1096
			 if (!ResourcePool.getInstance().bakExecResourceMap.containsKey(resourceType))
			 {
				 ResourcePool.getInstance().bakExecResourceMap.put(resourceType,resource);
			 }
		 }
		 ResourcePool.getInstance().resourceMap.put(resource,resourceType);
		 return out;
	 }
	 /**
	  * 移除资源
	  * @param taskId
	  * @param inMap
	  * @throws RemoteException
	  */
	 public MgloryOut removeResource(String taskId,Map<String,Object> inMap)
	 {
		 MgloryOut out =new MgloryOut();
		 String resource=inMap.get("resource").toString();
		 removeResource(resource);
		 return out;
		 
	 }
	 /**
	  * 移除资源
	  * @param resource
	  */
	 public void removeResource(String resource)
	 {
		 if (!ResourcePool.getInstance().resourceMap.containsKey(resource))
		 {
			 return;
		 }
		 if (ResourcePool.getInstance().resourceMap.get(resource).equals(ResourcePool.EXEC_RESOURCE))
		 {
			 //execResourceMap.remove(resource);
			 if (ResourcePool.getInstance().bakExecResourceMap.containsKey(resource))
			 {
				 //备用资源切换成在用资源
				 ResourcePool.getInstance().execResourceMap.put(resource, ResourcePool.getInstance().bakExecResourceMap.get(resource));
			 }
			 else
			 {
				 ResourcePool.getInstance().execResourceMap.remove(resource);
				 //数据迁移,暂未实现
			 }
		 }
		 else if (ResourcePool.getInstance().resourceMap.get(resource).equals(ResourcePool.FRONT_RESOURCE))
		 {
			 ResourcePool.getInstance().frontResourceMap.remove(resource);
		 }
		 else
		 {
			 if (ResourcePool.getInstance().bakExecResourceMap.containsValue(resource))
			 {
				 String bakResource="";
				 for (String key:ResourcePool.getInstance().bakExecResourceMap.keySet())
				 {
					 if (ResourcePool.getInstance().bakExecResourceMap.get(key).toString().equals(resource))
					 {
						 bakResource=key;
						 break;
					 }
				 }
				 if (!bakResource.equals(""))
				 {
					 ResourcePool.getInstance().bakExecResourceMap.remove(bakResource);
					 if(ResourcePool.getInstance().execResourceMap.get(bakResource).toString().equals(resource))
					 {
						 ResourcePool.getInstance().execResourceMap.remove(bakResource);
						 //数据迁移,暂未实现
					 }
				 }
			 }
		 }
		 ResourcePool.getInstance().resourceMap.remove(resource);
		 ResourcePool.getInstance().resourceMonitorMap.remove(resource);
		 ResourcePool.getInstance().resourceMonitorTimeMap.remove(resource);
		 
	 }
	 /**
	  * 获取primary资源
	  * @param taskId
	  * @param inMap
	  * @return
	  */
	 public MgloryOut getPrimary(String taskId,Map<String,Object> inMap)
	 {
		 MgloryOut out =new MgloryOut();
		 Map<String,Object> outMap=new HashMap<String,Object>();
		 outMap.put("primary",ElectManager.getInstance().getPrimary());
		 out.setMap(outMap);
		 return out;
	 }
	 /**
	  * 获取前置类资源
	  * @param taskId
	  * @param inMap
	  * @return
	  */
	 public MgloryOut getFrontResource(String taskId,Map<String,Object> inMap)
	 {
		 MgloryOut out =new MgloryOut();
		 Map<String,Object> outMap=new HashMap<String,Object>();
		 outMap.put("primary",ElectManager.getInstance().getPrimary());
		 outMap.put("front",Utils.transMap(ResourcePool.getInstance().frontResourceMap));
		 out.setMap(outMap);
		 return out;
	 }
	 /**
	  * 获取执行类资源
	  * @param taskId
	  * @param inMap
	  * @return
	  */
	 public MgloryOut getExecResource(String taskId,Map<String,Object> inMap)
	 {
		 MgloryOut out =new MgloryOut();
		 out.setMap(Utils.transMap(ResourcePool.getInstance().execResourceMap));
		 return out;
	 }
	 /**
	  * 获取执行类备份资源
	  * @param taskId
	  * @param inMap
	  * @return
	  */
	 public MgloryOut getBakExecResource(String taskId,Map<String,Object> inMap)
	 {
		 MgloryOut out =new MgloryOut();
		 out.setMap(Utils.transMap(ResourcePool.getInstance().bakExecResourceMap));
		 return out;
	 }
	 /**
	  * 获取resourceMonitorTime
	  * @param taskId
	  * @param inMap
	  * @return
	  */
	 public MgloryOut getResourceMonitorTime(String taskId,Map<String,Object> inMap)
	 {
		 MgloryOut out =new MgloryOut();
		 out.setMap(Utils.transMap(ResourcePool.getInstance().resourceMonitorTimeMap));
		 return out;
	 }
	 /**
	  * 获取resourceMonitorTime
	  * @return
	  */
	 public ConcurrentMap<String, Long> getResourceMonitorTime()
	 {
		 return ResourcePool.getInstance().resourceMonitorTimeMap;
	 }
	/**
	 * @param taskId
	 * @param inMap
	 * @return
	 * @throws Exception
	 */
	public MgloryOut synResuorce(String taskId,Map<String,Object> inMap)throws Exception
	{ 
		MgloryOut out =new MgloryOut();
		String resource=inMap.get("resource").toString();
		String resourceType=inMap.get("resourceType").toString();
		SystemResource systemResourc=(SystemResource)inMap.get("systemResource");
		Map<String,Object> map=new HashMap<String,Object>();
		map.put("resource", resource);
		map.put("resourceType", resourceType);
		addResource(taskId,map);
		ResourcePool.getInstance().resourceMonitorMap.put(resource, systemResourc);
		ResourcePool.getInstance().resourceMonitorTimeMap.put(resource,System.currentTimeMillis());
		Map<String,Object> outMap=new HashMap<String,Object>();
	    outMap.put("primary",ElectManager.getInstance().getPrimary());
	    out.setMap(outMap);
		return out;
	 }
	 /**
	  * 同步资源到前端机
	  * @param taskId
	  * @param inMap
	  * @return
	  * @throws Exception
	  */
	 @SuppressWarnings("unchecked")
	 public MgloryOut synResuorceToFront(String taskId,Map<String,Object> inMap)throws Exception
	 { 
		MgloryOut out =new MgloryOut();
		ConcurrentMap<String,String> resourceMap=(ConcurrentMap<String,String>)inMap.get("resourceMap");
		ConcurrentMap<String,String> execResourceMap=(ConcurrentMap<String,String>)inMap.get("execResourceMap");
		ConcurrentMap<String,String> bakExecResourceMap=(ConcurrentMap<String,String>)inMap.get("bakExecResourceMap");
		ConcurrentMap<String,SystemResource> resourceMonitorMap=(ConcurrentMap<String,SystemResource>)inMap.get("resourceMonitorMap");
		ConcurrentMap<String,Long> resourceMonitorTimeMap=(ConcurrentMap<String,Long>)inMap.get("resourceMonitorTimeMap");
		String primary=inMap.get("primary").toString();
		long primaryLease=Long.parseLong(inMap.get("primaryLease").toString());
		String elector=inMap.get("elector").toString();
		setResource(resourceMap,execResourceMap,bakExecResourceMap, resourceMonitorMap, resourceMonitorTimeMap);
		setPrimary(primary,primaryLease,elector);
		return out;
	 }
	 /**
	  * 设置资源
	  * @param resourceMap
	  * @param execResourceMap
	  * @param bakExecResourceMap
	  * @param serviceMap
	  * @param resourceMonitorMap
	  * @param resourceMonitorTimeMap
	  */
	 public void setResource(ConcurrentMap<String,String> resourceMap,ConcurrentMap<String,String> execResourceMap,
			 				 ConcurrentMap<String,String> bakExecResourceMap,ConcurrentMap<String,SystemResource> resourceMonitorMap,
			 				 ConcurrentMap<String,Long> resourceMonitorTimeMap)
	 {
		 ResourcePool.getInstance().resourceMap=resourceMap;
		 ResourcePool.getInstance().execResourceMap=execResourceMap;
		 ResourcePool.getInstance().bakExecResourceMap=bakExecResourceMap;
		 ResourcePool.getInstance().resourceMonitorMap=resourceMonitorMap;
		 ResourcePool.getInstance().resourceMonitorTimeMap=resourceMonitorTimeMap;
	 }
	 public void setPrimary(String primary,long primaryLease,String elector)
	 {
		 if (primaryLease>ElectManager.getInstance().getPrimaryLease())
		 {
			 NodeManager.getInstance().setPrimaryFlag(false);
			 ElectManager.getInstance().setPrimary(primary);
			 ElectManager.getInstance().setPrimaryLease(primaryLease);
			 ElectManager.getInstance().setElector(elector);
		 }
	 }
}
