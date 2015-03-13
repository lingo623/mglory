package com.lingo.mglory.param;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import com.lingo.mglory.task.TaskState;

public class WorkFlowDAG implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8470987561329561707L;
	private Map<String,Map<String,String>> taskMap=new HashMap<String,Map<String,String>>();//taskClazzName、operMethod、timeout
	private Map<String,List<String>> preTaskMap=new HashMap<String,List<String>>();
	private Map<String,List<String>> nextTaskMap=new HashMap<String,List<String>>();
	private ConcurrentMap<String,String> taskStateMap=new ConcurrentHashMap<String,String>();
	private ConcurrentMap<String,String> taskExcutorMap=new ConcurrentHashMap<String,String>();
	private ConcurrentMap<String,MgloryOut> taskResult=new ConcurrentHashMap<String,MgloryOut>();
	public WorkFlowDAG()
	{
		
	}
	public void setTask(String taskId,String taskClazzName,String operMethod,String timeout)
	{
		Map<String,String> taskInfoMap=new HashMap<String,String>();
		taskInfoMap.put("taskClazzName", taskClazzName);//执行类
		taskInfoMap.put("operMethod", operMethod);//执行方法
		taskInfoMap.put("timeout", timeout);//服务器执行超时时间
		taskMap.put(taskId, taskInfoMap);
		setTaskState(taskId,TaskState.WAIT_RUN);
	}
	public void setPreTask(String taskId,String preTaskId)
	{
		if (preTaskMap.get(taskId)==null)
		{
			List<String> list=new LinkedList<String>();
			list.add(preTaskId);
			preTaskMap.put(taskId, list);
		}
		else
		{
			List<String> list=preTaskMap.get(taskId);
	    	list.add(preTaskId);
	    	preTaskMap.put(taskId,list);
		}
	}
	public void setNextTask(String taskId,String nextTaskId)
	{
		if (nextTaskMap.get(taskId)==null)
		{
			List<String> list=new LinkedList<String>();
			list.add(nextTaskId);
			nextTaskMap.put(taskId, list);
		}
		else
		{
			List<String> list=nextTaskMap.get(taskId);
	    	list.add(nextTaskId);
	    	nextTaskMap.put(taskId,list);
		}
	}
	public void setTaskResult(String taskId,MgloryOut out)
	{
		taskResult.put(taskId,out);
	}
	public void setTaskState(String taskId,String state)
	{
		taskStateMap.put(taskId, state);
	}
	public void setTaskExcutor(String taskId,String resource)
	{
		taskExcutorMap.put(taskId, resource);
	}
	public Map<String,Map<String,String>> getTaskMap()
	{
		return taskMap;
	}
	public Map<String,List<String>> getPreTaskMap()
	{
		return preTaskMap;
	}
	public Map<String,List<String>> getNextTaskMap()
	{
		return nextTaskMap;
	}
	public ConcurrentMap<String,MgloryOut> getTaskResult()
	{
		return taskResult;
	}
	public ConcurrentMap<String,String> getTaskStateMap()
	{
		return taskStateMap;
	}
	public ConcurrentMap<String,String> getTaskExcutorMap()
	{
		return taskExcutorMap;
	}
	public void interruptTaskState()
	{
		for (String taskId:taskStateMap.keySet())
		{
			if (taskStateMap.get(taskId).equals(TaskState.WAIT_RUN) || taskStateMap.get(taskId).equals(TaskState.RUNNING))
			{
				taskStateMap.put(taskId, TaskState.INTERRUPT);
			}
		}
	}
}
