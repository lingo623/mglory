package com.lingo.mglory.param;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MgloryIn implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3111696841231705271L;
	private String taskId;
	private Map<String,Object> map;
	private String invokeMethod;
	private long timeout;
	public MgloryIn()
	{
		taskId="";
		map=new HashMap<String,Object>();
		timeout=0;
	}
	public void setMap(Map<String,Object> map)
	{
		this.map=map;
	}
	public Map<String,Object> getMap()
	{
		return map;
	}
	public void setTaskId(String taskId)
	{
		this.taskId=taskId;
	}
	public String getTaskId()
	{
		return taskId;
	}
	public void setInvokeMethod(String invokeMethod)
	{
		this.invokeMethod=invokeMethod;
	}
	public String getInvokeMethod()
	{
		return invokeMethod;
	}
	public void setTimeout(long timeout)
	{
		this.timeout=timeout;
	}
	public long getTimeout()
	{
		return timeout;
	}
}
