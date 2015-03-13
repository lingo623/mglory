package com.lingo.mglory.resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResourceUsage {
	private static Log log = LogFactory.getLog(ResourceUsage.class);
	public static SystemResource getResUsage() throws Exception {
        SystemResource resUsage= new SystemResource();    
        resUsage.setCpuRatio(getCpuUsage());
        resUsage.setFreeMemory(getFreeMemory());
        resUsage.setIoRatio(getIoUsage());
        resUsage.setNetRatio(getNetUsage());
        return resUsage;
	}
	/**
	 * Purpose:采集CPU使用率
	 * @param args
	 * @return float,CPU使用率,小于1
	 */
	public static float getCpuUsage() {
		//log.info("开始收集cpu使用率");
		float cpuUsage = 0;
		Process pro1,pro2;
		Runtime r = Runtime.getRuntime();
		try {
			String command = "cat /proc/stat";
			pro1 = r.exec(command);
			BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
			String line = null;
			long idleCpuTime1 = 0, totalCpuTime1 = 0;	//分别为系统启动后空闲的CPU时间和总的CPU时间
			while((line=in1.readLine()) != null){	
				if(line.startsWith("cpu")){
					line = line.trim();
					//log.info(line);
					String[] temp = line.split("\\s+"); 
					idleCpuTime1 = Long.parseLong(temp[4]);
					for(String s : temp){
						if(!s.equals("cpu")){
							totalCpuTime1 += Long.parseLong(s);
						}
					}	
					//log.info("IdleCpuTime: " + idleCpuTime1 + ", " + "TotalCpuTime" + totalCpuTime1);
					break;
				}						
			}	
			in1.close();
			pro1.destroy();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				log.error("CpuUsage休眠时发生InterruptedException. " + e.getMessage());
				log.error(sw.toString());
			}
			pro2 = r.exec(command);
			BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
			long idleCpuTime2 = 0, totalCpuTime2 = 0;	//分别为系统启动后空闲的CPU时间和总的CPU时间
			while((line=in2.readLine()) != null){	
				if(line.startsWith("cpu")){
					line = line.trim();
					//log.info(line);
					String[] temp = line.split("\\s+"); 
					idleCpuTime2 = Long.parseLong(temp[4]);
					for(String s : temp){
						if(!s.equals("cpu")){
							totalCpuTime2 += Long.parseLong(s);
						}
					}
					//log.info("IdleCpuTime: " + idleCpuTime2 + ", " + "TotalCpuTime" + totalCpuTime2);
					break;	
				}								
			}
			if(idleCpuTime1 != 0 && totalCpuTime1 !=0 && idleCpuTime2 != 0 && totalCpuTime2 !=0){
				cpuUsage = 1 - (float)(idleCpuTime2 - idleCpuTime1)/(float)(totalCpuTime2 - totalCpuTime1);
				//log.info("本节点CPU使用率为: " + cpuUsage);
			}				
			in2.close();
			pro2.destroy();
		} catch (IOException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			log.error("CpuUsage发生InstantiationException. " + e.getMessage());
			log.error(sw.toString());
		}	
		return cpuUsage;
	}
	/**
	 * Purpose:采集空闲内存
	 * @param args
	 * @return 
	 */
	public static long getFreeMemory() {
		//log.info("开始收集空闲内存");
		//float memUsage = 0.0f;
		Process pro = null;
		Runtime r = Runtime.getRuntime();
		//long totalMem = 0, freeMem = 0;
		long freeMem=0;
		try {
			String command = "cat /proc/meminfo";
			pro = r.exec(command);
			BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
			String line = null;
			int count = 0;
 			while((line=in.readLine()) != null){	
				//log.info(line);	
				String[] memInfo = line.split("\\s+");
//				if(memInfo[0].startsWith("MemTotal")){
//					totalMem = Long.parseLong(memInfo[1]);
//				}
				if(memInfo[0].startsWith("MemFree")){
					freeMem = Long.parseLong(memInfo[1]);
				}
//				memUsage = 1- (float)freeMem/(float)totalMem;
//				log.info("本节点内存使用率为: " + memUsage);	
				if(++count == 2){
					break;
				}				
			}
			in.close();
			pro.destroy();
		} catch (IOException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			log.error("MemUsage发生InstantiationException. " + e.getMessage());
			log.error(sw.toString());
		}
		//log.info("空闲内存:"+freeMem);
		return freeMem;
	}
	/**
	 * @Purpose:采集磁盘IO使用率
	 * @param args
	 * @return float,磁盘IO使用率,小于1
	 */
	public static float getIoUsage() {
		//log.info("开始收集磁盘IO使用率");
		float ioUsage = 0.0f;
		Process pro = null;
		Runtime r = Runtime.getRuntime();
		try {
			String command = "iostat -d -x";
			pro = r.exec(command);
			BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
			String line = null;
			int count =  0;
			while((line=in.readLine()) != null){		
				if(++count >= 4){
//					log.info(line);
					String[] temp = line.split("\\s+");
					if(temp.length > 1){
						float util =  Float.parseFloat(temp[temp.length-1]);
						ioUsage = (ioUsage>util)?ioUsage:util;
					}
				}
			}
			if(ioUsage > 0){
				//log.info("本节点磁盘IO使用率为: " + ioUsage);	
				ioUsage /= 100; 
			}			
			in.close();
			pro.destroy();
		} catch (IOException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			log.error("IoUsage发生InstantiationException. " + e.getMessage());
			log.error(sw.toString());
		}	
		return ioUsage;
	}
	/**
	 * @Purpose:采集网络带宽使用率
	 * @param args
	 * @return float,网络带宽使用率,小于1
	 */
	public static float getNetUsage() {
		float TotalBandwidth = 1000;	//网口带宽,Mbps
		//log.info("开始收集网络带宽使用率");
		float netUsage = 0.0f;
		Process pro1,pro2;
		Runtime r = Runtime.getRuntime();
		try {
			String command = "cat /proc/net/dev";
			//第一次采集流量数据
			long startTime = System.currentTimeMillis();
			pro1 = r.exec(command);
			BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
			String line = null;
			long inSize1 = 0, outSize1 = 0;
			while((line=in1.readLine()) != null){	
				line = line.trim();
				if(line.startsWith("eth0")){
					//log.info(line);
					String[] temp = line.split("\\s+"); 
					inSize1 = Long.parseLong(temp[0].substring(5));	//Receive bytes,单位为Byte
					outSize1 = Long.parseLong(temp[8]);				//Transmit bytes,单位为Byte
					break;
				}				
			}	
			in1.close();
			pro1.destroy();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				log.error("NetUsage休眠时发生InterruptedException. " + e.getMessage());
				log.error(sw.toString());
			}
			//第二次采集流量数据
			long endTime = System.currentTimeMillis();
			pro2 = r.exec(command);
			BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
			long inSize2 = 0 ,outSize2 = 0;
			while((line=in2.readLine()) != null){	
				line = line.trim();
				if(line.startsWith("eth0")){
					//log.info(line);
					String[] temp = line.split("\\s+"); 
					inSize2 = Long.parseLong(temp[0].substring(5));
					outSize2 = Long.parseLong(temp[8]);
					break;
				}				
			}
			if(inSize1 != 0 && outSize1 !=0 && inSize2 != 0 && outSize2 !=0){
				float interval = (float)(endTime - startTime)/1000;
				//网口传输速度,单位为bps
				float curRate = (float)(inSize2 - inSize1 + outSize2 - outSize1)*8/(1000000*interval);
				netUsage = curRate/TotalBandwidth;
				//log.info("本节点网口速度为: " + curRate + "Mbps");
				//log.info("本节点网络带宽使用率为: " + netUsage);
			}				
			in2.close();
			pro2.destroy();
		} catch (IOException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			log.error("NetUsage发生InstantiationException. " + e.getMessage());
			log.error(sw.toString());
		}	
		return netUsage;
	}
}
