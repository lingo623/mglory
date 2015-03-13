package com.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.job.JobExecutor;
import com.lingo.mglory.param.WorkFlowDAG;
import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.util.Utils;
public class WordCount2{
	//private static Log log = LogFactory.getLog(WordCount.class);
	public WordCount2() {
		
	}
	public static void main(String[] args)throws Exception
	{
		WordCount2 test=new WordCount2();
		test.syn();
		System.exit(0);
	}
	public void syn()
	{
		ExecutorService taskExecutor=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		List<FutureTask<MgloryOut>> futureTasks = new ArrayList<FutureTask<MgloryOut>>();
		for (int i=0;i<Runtime.getRuntime().availableProcessors();i++)
		{
			FutureTask<MgloryOut> futureTask = new FutureTask<MgloryOut>(
					new Callable<MgloryOut>() {
						@Override
						public MgloryOut call() throws Exception {
							return test();
						}
			});
			futureTasks.add(futureTask);
			taskExecutor.submit(futureTask);
		}
		for (FutureTask<MgloryOut> futureTask : futureTasks) {
			try {
				futureTask.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		taskExecutor.shutdown();
	}
	public MgloryOut test() throws Exception
	{
		long t=System.currentTimeMillis();
		Map<String,Object> paramMap=new HashMap<String,Object>();
		String taskId1="map1"+Utils.getSeqId();
		String taskId2="map2"+Utils.getSeqId();
		paramMap.put(taskId1, "Hello World Bye World Hello Hadoop GoodBye Hadoop");
		paramMap.put(taskId2, "Happy New Year Today Is Good Day Hello Through producing different" +
				" mature RNAs from one same gene alternative splicing (AS) helps improve the capacity " +
				"of transcriptome and proteome  It has been proposed that human genome permits more AS " +
				"events which explains the fact that human are far more complex than lower animals with " +
				"similar gene numbers (such as C elegans)  By taking advantage of high throughput transcriptome " +
				"sequencing (RNA-seq) technology researchers have reported that at least 95% of human multi-exon " +
				"genes are alternative spliced  Such result seems supporting the former hypothesis  However it’s " +
				"still unclear whether all these AS events are functional");
		MgloryIn in=new MgloryIn();
		in.setMap(paramMap);
		in.setInvokeMethod(WordCount.class.getName()+".map");
		WorkFlowDAG dag=new WorkFlowDAG();
		dag.setTask("start", "", "", "");
		dag.setTask("end", "", "", "");
		//String taskId1="map1"+Utils.getSeqId();
		dag.setTask(taskId1, WordCount.class.getName(), "map", "0");
		dag.setPreTask(taskId1, "start");
		//dag.setPreTask("end", taskId1);
		dag.setNextTask("start", taskId1);
		//dag.setNextTask(taskId1, "end");
		
		//String taskId2="map2"+Utils.getSeqId();
		dag.setTask(taskId2, WordCount.class.getName(), "map", "0");
		dag.setPreTask(taskId2, "start");
		//dag.setPreTask("end", taskId2);
		dag.setNextTask("start", taskId2);
		//dag.setNextTask(taskId2, "end");
		
		
		String taskId3="reduce"+Utils.getSeqId();
		dag.setTask(taskId3,  WordCount.class.getName(), "reduce", "0");
		dag.setPreTask(taskId3, taskId1);
		dag.setPreTask(taskId3, taskId2);
		dag.setPreTask("end", taskId3);
		dag.setNextTask(taskId1, taskId3);
		dag.setNextTask(taskId2, taskId3);
		dag.setNextTask(taskId3, "end");
		
		JobExecutor jobExecutor=new JobExecutor("114.215.178.115:1099","114.215.178.115:1099");
		MgloryOut out =jobExecutor.execute(in, dag);
		System.out.println("map==>"+out.getMap());
		System.out.println("status==>"+out.getStatus());
		System.out.println("msg==>"+out.getErrorMsg());
		System.out.println("耗时===>"+(System.currentTimeMillis()-t));
		return new MgloryOut();
	}
}
