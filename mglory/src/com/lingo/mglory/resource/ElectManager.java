package com.lingo.mglory.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.server.MgloryGroupServer;
import com.lingo.mglory.transport.client.Client;

public class ElectManager {
	private static Log log = LogFactory.getLog(ElectManager.class);
	private static ElectManager electManager;
	private String primary="";
	private long primaryLease=0;
	private String elector="";
	private int validateNum=0;
	private ElectManager()
	{
		
	}
	public synchronized static ElectManager getInstance()
	{
		if (electManager==null)
		{
			electManager=new ElectManager();
		}
		return electManager;
	}
	public String getPrimary()
	{
		return primary;
	}
	public void setPrimary(String primary)
	{
		this.primary=primary;
	}
	public long getPrimaryLease()
	{
		return primaryLease;
	}
	public void setPrimaryLease(long primaryLease)
	{
		this.primaryLease=primaryLease;
	}
	public String getElector()
	{
		return elector;
	}
	public void setElector(String elector)
	{
		this.elector=elector;
	}
	/**
	 * 选举primary
	 * @return
	 */
	 public void electPrimary()
	 {
		 	String primaryWaitElect=getPrimary();
			String masterGroup=ResourcePool.getInstance().getMasterGroup();
			String slaveGroup=ResourcePool.getInstance().getSlaveGroup();
			String localResource=MgloryGroupServer.getInstance().getIp()+":"+MgloryGroupServer.getInstance().getPort();
			if (primaryWaitElect.equals(""))
			{
				primaryWaitElect=localResource;
			}
			if (getPrimary().equals(masterGroup)
			 || getPrimary().equals(slaveGroup))
			{
				ResourceManager resourceManager=new ResourceManager();
				ConcurrentMap<String,String> frontResourceMap=resourceManager.getFrontResource();
				if (!frontResourceMap.isEmpty())
				{
					primaryWaitElect=frontResourceMap.keySet().toArray()[0].toString();
				}
			}
			if (validateNum>5)
			{
				//重新选举
				ResourceManager resourceManager=new ResourceManager();
				ConcurrentMap<String,String> frontResourceMap=resourceManager.getFrontResource();
				if (!frontResourceMap.isEmpty())
				{
					for (String key:frontResourceMap.keySet())
					{
						if (key!=primaryWaitElect)
						{
							primaryWaitElect=key;
							break;
						}
					}
				}
				//没选到前端机时，选举本地资源为primary
				if(primaryWaitElect.equals(getPrimary()))
				{
					primaryWaitElect=localResource;
				}
				
			}
			if(!primaryWaitElect.equals(getPrimary()))
			{
				setPrimary(primaryWaitElect);
				setPrimaryLease(System.currentTimeMillis());
			}
			if (!primaryWaitElect.equals(localResource))
			{
				//校验
				MgloryIn mgloryIn=new MgloryIn();
				Map<String,Object> inMap=new HashMap<String,Object>();
				inMap.put("elector", localResource);
				inMap.put("primary", getPrimary());
				inMap.put("primaryLease", getPrimaryLease());
				mgloryIn.setMap(inMap);
				mgloryIn.setInvokeMethod("com.lingo.mglory.resource.ResourceScheduler.synPrimary");
				try
				{
					String ip=primary.split(":")[0];
					int port=Integer.parseInt(primary.split(":")[1]);
					MgloryOut out=Client.getInstance().call(ip,port,mgloryIn);
					String elector=out.getMap().get("elector").toString();
					NodeManager.getInstance().setElectFlag(elector.equals(localResource));
					validateNum=0;
				}
				catch(Exception e)
				{
					log.error("校验primary失败===>"+e);
					validateNum++;
				}
			}
			else
			{
				//资源为primary
				NodeManager.getInstance().setPrimaryFlag(true);
			}
	 }
	 
}
