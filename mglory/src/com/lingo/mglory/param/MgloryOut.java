package com.lingo.mglory.param;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MgloryOut implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6822952442841004132L;
	private String taskId;
	private Map<String,Object> map;
	private String status;
	private String errorMsg;
	private String operMethod;
	private String storeLocation;
	private int storeNum;
	public static final String NORMAL="NORMAL";//正常
	public static final String EXCEPTION="EXCEPTION";//异常
	public static final String INTERRUPT="INTERRUPT";//中断
	public static final String RESULT="RESULT";//直接结果
	public static final String LOCAL_MEM="LOCAL_MEM";//本地内存
	public static final String LOCAL_FILE="LOCAL_FILE";//本地文件
	public static final String STORE_MEM="STORE_MEM";//内存存储系统
	public MgloryOut()
	{
		taskId="";
		map=new HashMap<String,Object>();
		status=NORMAL;
		errorMsg="";
		operMethod="";
		storeLocation=RESULT;
		storeNum=1;
	}
	public void setMap(Map<String,Object> map)
	{
		this.map=map;
	}
	public Map<String,Object> getMap()
	{
		return map;
	}
	public void setStatus(String status)
	{
		this.status=status;
	}
	public String getStatus()
	{
		return status;
	}
	public void setErrorMsg(String errorMsg)
	{
		this.errorMsg=errorMsg;
	}
	public String getErrorMsg()
	{
		return errorMsg;
	}
	public void setOperMethod(String operMethod)
	{
		this.operMethod=operMethod;
	}
	public String getOperMethod()
	{
		return operMethod;
	}
	public void setStoreLocation(String storeLocation)
	{
		this.storeLocation=storeLocation;
	}
	public String getStoreLocation()
	{
		return storeLocation;
	}
	public void setTaskId(String taskId)
	{
		this.taskId=taskId;
	}
	public String getTaskId()
	{
		return taskId;
	}
	public void setStoreNum(int storeNum)
	{
		this.storeNum=storeNum;
	}
	public int getStoreNum()
	{
		return storeNum;
	}
}
