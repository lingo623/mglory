/*******************************************************************************
 * @Description:中心服务类 (资源管理) 
 * @author sunxuhang
 * @revision 1.0
 * @date 2013-12-17
 *******************************************************************************/
package com.lingo.mglory.server; 
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.resource.ResourcePool;
import com.lingo.mglory.transport.server.Server;
import com.lingo.mglory.util.PropertiesUtil;
public class MgloryGroupServer {
	private static Log log = LogFactory.getLog(MgloryGroupServer.class);
	private static MgloryGroupServer groupServer;
	private String ip;
	private int port;
	private boolean masterFlag=false;
	private Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
	private MgloryGroupServer()
	{
		init();
	}
	private void init()
	{
		if (config.getProperty("masterFlag")!=null)
		{
			masterFlag=Boolean.parseBoolean(config.getProperty("masterFlag"));
		}
		if (masterFlag)
		{
			ip=config.getProperty("masterIp");
			port=Integer.parseInt(config.getProperty("masterPort"));
		}
		else
		{
			if (config.getProperty("slavePort")!=null)
			{
				ip=config.getProperty("slaveIp");
				port=Integer.parseInt(config.getProperty("slavePort"));
			}
			else
			{
				throw new RuntimeException("请配置slave服务器IP、端口");
			}
		}
		String masterGroup=config.getProperty("masterIp")+":"+config.getProperty("masterPort");
		String slaveGroup=config.getProperty("slaveIp")+":"+config.getProperty("slavePort");
		ResourcePool.getInstance().setMasterGroup(masterGroup);
		ResourcePool.getInstance().setSlaveGroup(slaveGroup);
	}
	public synchronized static MgloryGroupServer getInstance()
	{
		if (groupServer==null)
		{
			groupServer=new MgloryGroupServer();
		}
		return groupServer;
	}
	/**
	 * 启动中心管理服务
	 */
	public void startGroupServer() throws Exception
	{
		startGroupServer(ip,port);
	}
	public void startGroupServer(String ip,int port) throws Exception
	{
		try
		{
			Server.start(ip, port);
		}
		catch(Exception e)
		{
			log.error("管理服务启动失败===>"+e);
			throw new Exception("管理服务启动失败:"+e);
		}
	}
	public boolean getMasterFlag()
	{
		return masterFlag;
	}
	public String getIp()
	{
		return ip;
	}	
	public int getPort()
	{
		return port;
	}
}
