package com.test;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.job.JobExecutor;
import com.lingo.mglory.param.WorkFlowDAG;
import com.lingo.mglory.param.MgloryIn;
import com.lingo.mglory.param.MgloryOut;
import com.lingo.mglory.util.Utils;
public class WordCount{
	private static Log log = LogFactory.getLog(WordCount.class);
	public WordCount() {
		
	}
	public MgloryOut map(String taskId,Map<String,Object> inMap) throws Exception
	{
		MgloryOut out=new MgloryOut();
		Map<String,Object> collector=new HashMap<String,Object>();
		for (String preTaskId:inMap.keySet())
		{
			Map<String,Object> paramMap=((MgloryOut)inMap.get(preTaskId)).getMap();
			String text=paramMap.get(taskId).toString();
			StringTokenizer tokenizer = new StringTokenizer(text);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				if (collector.containsKey(word))
				{
					collector.put(word, Integer.parseInt(collector.get(word).toString())+1);
				}
				else
				{
					collector.put(word, 1);
				}
			}
			
		}
		log.info("collector===>"+collector);
		out.setMap(collector);
		return out;
	}
	public MgloryOut reduce(String taskId,Map<String,Object> inMap) throws Exception
	{
		MgloryOut out=new MgloryOut();
		Map<String,Object> collector=new HashMap<String,Object>();
		for (String taskId1:inMap.keySet())
		{
			Map<String,Object> paramMap=((MgloryOut)inMap.get(taskId1)).getMap();
			for (String word:paramMap.keySet())
			{
				if (collector.containsKey(word))
				{
					collector.put(word, Integer.parseInt(collector.get(word).toString())+Integer.parseInt(paramMap.get(word).toString()));
				}
				else
				{
					collector.put(word,paramMap.get(word));
				}
			}
		}
		log.info("collector===>"+collector);
		out.setMap(collector);
		return out;
	}
	public static void main(String[] args)throws Exception
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
		System.exit(0);
	}
}
