package com.lingo.mglory.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.WorkFlowDAG;
import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.resource.ResourceHelper;
import com.lingo.mglory.task.TaskState;
import com.lingo.mglory.transport.client.Client;

public class JobExecutor {
	private static Log log = LogFactory.getLog(JobExecutor.class);
	private String masterGroup;//中心服务（资源管理器）主机地址 如：114.215.178.115:1099
	private String slaveGroup;//中心服务（资源管理器）备机地址
	private WorkFlowDAG dag;
	private boolean isInterrupted=false;
	private static ExecutorService taskExecutor=null;
	static
	{
		taskExecutor=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}
	public JobExecutor(String masterGroup,String slaveGroup)
	{
		this.masterGroup=masterGroup;
		this.slaveGroup=slaveGroup;
	}
	/**
	 * 任务执行
	 * @param in
	 * @return
	 */
	public MgloryOut execute(MgloryIn in)
	{
		return execute(in,"");
	}
	/**
	 * 任务执行
	 * @param in
	 * @param execResource
	 * @return
	 */
	public MgloryOut execute(MgloryIn in,String execResource)
	{
		String taskId=in.getTaskId();
		MgloryOut out =new  MgloryOut();
		out.setTaskId(taskId);
		try
		{
			String resource=execResource;
			if (resource.equals(""))
			{
				resource=ResourceHelper.getInstance().getIdleResource(masterGroup, slaveGroup);
			}
			dag.setTaskExcutor(taskId,resource);
			String ip=resource.split(":")[0];
			int port=Integer.parseInt(resource.split(":")[1]);
			out=Client.getInstance().call(ip, port,in);
			out.setTaskId(taskId);
		}
		catch(Exception e)
		{
			out.setTaskId(taskId);
			out.setStatus(MgloryOut.EXCEPTION);
			out.setErrorMsg(e.getMessage());
			log.warn("忽略执行任务异常："+e);
		}
	    return out;
	}
	/**
	 * 任务执行
	 * @param in
	 * @param dag
	 * @return
	 * @throws Exception
	 */
	public MgloryOut execute(MgloryIn in,WorkFlowDAG dag) throws Exception
	{
		MgloryOut out =new  MgloryOut();
		if (dag==null || dag.getTaskMap().isEmpty())
		{
			out.setErrorMsg("空DAG无须执行");
			return out;
		}
		this.dag=dag;
		preDealForStartTaskNode(in);
		execute("start");
		if (isInterrupted)
		{
			interrupt();
		}
		out=dag.getTaskResult().get("end");
		return out;
	}
	private void execute(String taskId)
	{
		Map <String,String> needDealTaskMap=getNeedDealTask(taskId);
		if (!needDealTaskMap.isEmpty())
		{
			//end任务节点不处理
			if(needDealTaskMap.containsKey("end"))
			{
				dag.setTaskState(taskId,TaskState.COMPLETE);
				//taskExecutor.shutdown();
				return;
			}
			//ExecutorService taskExecutor=Executors.newFixedThreadPool(needDealTaskMap.size());
			List<FutureTask<MgloryOut>> futureTasks = new ArrayList<FutureTask<MgloryOut>>();
			for (final String waitExecuteTaskId:needDealTaskMap.keySet())
			{
				final String taskClazzName=dag.getTaskMap().get(waitExecuteTaskId).get("taskClazzName");
				final List<String> preTaskList=dag.getPreTaskMap().get(waitExecuteTaskId);
				final String operMethod=dag.getTaskMap().get(waitExecuteTaskId).get("operMethod");
				final long timeout=Long.parseLong(dag.getTaskMap().get(waitExecuteTaskId).get("timeout"));
				dag.setTaskState(waitExecuteTaskId,TaskState.RUNNING);
				FutureTask<MgloryOut> futureTask = new FutureTask<MgloryOut>(
						new Callable<MgloryOut>() {
							@Override
							public MgloryOut call() throws Exception {
								String execResource="";
								String locationType=MgloryOut.RESULT;
								MgloryIn in=new MgloryIn();
								in.setTaskId(waitExecuteTaskId);
								in.setInvokeMethod(taskClazzName+"."+operMethod);
								Map<String,Object> inMap=new HashMap<String,Object>();
								for (String preTaskId:preTaskList)
								{
									MgloryOut out=dag.getTaskResult().get(preTaskId);
									inMap.put(preTaskId,out);
									if (out.getStoreLocation().equals(MgloryOut.LOCAL_MEM) && !locationType.equals(MgloryOut.LOCAL_FILE)
									   && execResource.equals(""))
									{
										execResource=out.getMap().get("locationAddress")!=null?out.getMap().get("locationAddress").toString():"";
									}
									else if (out.getStoreLocation().equals(MgloryOut.LOCAL_FILE) && !locationType.equals(MgloryOut.LOCAL_FILE))
									{
										if (out.getMap().get("locationAddress")!=null)
										{
											execResource=out.getMap().get("locationAddress").toString();
											locationType=MgloryOut.LOCAL_FILE;
										}
									}
								}
								in.setMap(inMap);
								in.setTimeout(timeout);
								return execute(in,execResource);
							}
				});
				futureTasks.add(futureTask);
				taskExecutor.submit(futureTask);
			}
			for (FutureTask<MgloryOut> futureTask : futureTasks) {
				try {
					if (!isInterrupted)
					{
						MgloryOut out=futureTask.get();
						if (out.getStatus().equals(MgloryOut.NORMAL))
						{
							dag.setTaskState(out.getTaskId(),TaskState.COMPLETE);
							dag.setTaskResult(out.getTaskId(),out);
							if (dag.getNextTaskMap().get(out.getTaskId())!=null && dag.getNextTaskMap().get(out.getTaskId()).get(0).equals("end"))
							{
								dag.setTaskResult("end",out);
							}
							execute(out.getTaskId());
						}
						else
						{
							dag.setTaskState(out.getTaskId(),TaskState.ERROR);
							dag.setTaskResult(out.getTaskId(),out);
							dag.setTaskResult("end",out);
							//终止任务执行
							if(!futureTask.isCancelled())
							{
								futureTask.cancel(true);
							}
							isInterrupted=true;
						}
					}
					else
					{
						//终止任务执行
						if(!futureTask.isCancelled())
						{
							futureTask.cancel(true);
						}
					}
					
				} catch (Exception e) {
					if (!isInterrupted)
					{
						MgloryOut out=new MgloryOut();
						out.setStatus(MgloryOut.EXCEPTION);
						out.setErrorMsg(e.getMessage());
						dag.setTaskResult("end",out);
						isInterrupted=true;
						log.warn("忽略执行任务异常："+e);
					}

				}
			}
			//taskExecutor.shutdown();
		}
	}
	private void preDealForStartTaskNode(MgloryIn in)
	{
		dag.setTaskState("start",TaskState.COMPLETE);
		MgloryOut startOut =new  MgloryOut();
		startOut.setTaskId("start");
		startOut.setMap(in.getMap());
		dag.setTaskResult("start", startOut);
	}
	private Map<String,String> getNeedDealTask(String taskId)
	{
		Map <String,String> needDealTaskMap=new HashMap <String,String>();
		//获取孩子节点
		List<String> nextTaskList=dag.getNextTaskMap().get(taskId);
		if (nextTaskList!=null && nextTaskList.size()>0)
		{
			 for (int i=0;i<nextTaskList.size();i++)
			 {
				 String childTaskId=nextTaskList.get(i);
				 List<String> preTaskList=dag.getPreTaskMap().get(childTaskId);
				 //判断是否可以执行
				 boolean flag=true;
				 for (int j=0;j<preTaskList.size();j++)
				 {
					 String parentTaskId=preTaskList.get(j);
					 if (!dag.getTaskStateMap().get(parentTaskId).equals(TaskState.COMPLETE))
					 {
						 flag=false;
						 break;
					 }
				 }
				 if (flag)
				 {
					 needDealTaskMap.put(childTaskId, childTaskId);
				 }
			 }
		}
		return needDealTaskMap;
	}
	//中断服务器上的任务执行
	private void interrupt()
	{
		dag.interruptTaskState();
		//暂不实现
	}
}
