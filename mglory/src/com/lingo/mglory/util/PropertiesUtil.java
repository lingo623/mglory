package com.lingo.mglory.util;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.server.MgloryGroupServer;

public class PropertiesUtil {
	private static Log log = LogFactory.getLog(MgloryGroupServer.class);
	private static PropertiesUtil propertiesUtil;
	private static Map<String,Properties> configMap=new HashMap<String,Properties>();
	public synchronized static PropertiesUtil getInstance()
	{
		if (propertiesUtil==null)
		{
			propertiesUtil=new PropertiesUtil();
		}
		return propertiesUtil;
	}
	public Properties getConfig(String fileName)
	{
		Properties config=new Properties();
		if (configMap.get(fileName)!=null)
		{
			config= configMap.get(fileName);
		}
		else
		{
			String configFile=PropertiesUtil.class.getClassLoader().getResource(fileName).getPath().toString();
			configFile=configFile.replaceAll("%20", " ");
			FileInputStream inputStream=null;
			try
			{
				inputStream=new FileInputStream(configFile);
				config.load(inputStream);
				configMap.put(fileName,config);
			}
			catch(Exception e)
			{
				log.error("load properties error id :"+e.getLocalizedMessage());
			}
			finally
			{
				if(inputStream!=null)
				{
					try
					{
						inputStream.close();
					}
					catch(Exception e)
					{
						log.error("load properties error id :"+e.getLocalizedMessage());
					}
				}
			}
		}
		
		return config;
	}
}