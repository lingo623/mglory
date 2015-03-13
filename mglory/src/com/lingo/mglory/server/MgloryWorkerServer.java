/*******************************************************************************
 * @Description:执行服务类(负责具体事务) 
 * @author sunxuhang
 * @revision 1.0
 * @date 2013-12-17
 *******************************************************************************/
package com.lingo.mglory.server;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.transport.server.Server;
import com.lingo.mglory.util.PropertiesUtil;
public class MgloryWorkerServer {
	private static Log log = LogFactory.getLog(MgloryWorkerServer.class);
	private Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
	private static MgloryWorkerServer workerServer;
	private String masterGroup;
	private String slaveGroup;
	private String resource;
	private String resourceType;
	private MgloryWorkerServer()
	{
		init();
	}
	private void init()
	{
		String masterIp=config.getProperty("masterIp");
		String masterPort=config.getProperty("masterPort");
		String slaveIp=config.getProperty("slaveIp");
		String slavePort=config.getProperty("slavePort");
		masterGroup=masterIp+":"+masterPort;
		slaveGroup=slaveIp+":"+slavePort;
	}
	public synchronized static MgloryWorkerServer getInstance()
	{
		if(workerServer==null)
		{
			workerServer=new MgloryWorkerServer();
		}
		return workerServer;
	}
	/**
	 * 注册资源
	 * @param ip
	 * @param port
	 * @param resourceType
	 */
	public void startResource(String ip,int port,String resourceType)throws Exception
	{
		try
		{
			Server.start(ip, port);
		}
		catch(Exception e)
		{
			log.error("启动工作服务器失败===>"+e);
			throw new Exception("启动工作服务器失败===>"+e);
		}
		resource=ip+":"+port;
		this.resourceType=resourceType;
	}
	public String getMasterGroup()
	{
		return masterGroup;
	}
	public String getSlaveGroup()
	{
		return slaveGroup;
	}
	public String getResource()
	{
		return resource;
	}
	public String getResourceType()
	{
		return resourceType;
	}
}
