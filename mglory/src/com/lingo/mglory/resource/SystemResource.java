package com.lingo.mglory.resource;

import java.io.Serializable;


public class SystemResource implements Serializable{

	  private static final long serialVersionUID = -1553098501230376578L;
  
	    
	  /**  剩余内存 */    
	   private long freeMemory;      
	     
	   /** cpu使用率 */    
	   private float cpuRatio;
	   
	   /** 磁盘IO使用率 */    
	   private float ioRatio;
	   
	   /** 网络带宽使用率 */    
	   private float netRatio;
	 
	   public long getFreeMemory() {    
	       return freeMemory;    
	   }    
	 
	   public void setFreeMemory(long freeMemory) {    
	       this.freeMemory = freeMemory;    
	   }
	 
	   public float getCpuRatio() {    
	       return cpuRatio;    
	   }    
	  
	   public void setCpuRatio(float cpuRatio) {    
	       this.cpuRatio = cpuRatio;    
	   }
	   
	   public float getIoRatio() {    
	       return ioRatio;    
	   }    
	  
	   public void setIoRatio(float ioRatio) {    
	       this.ioRatio = ioRatio;    
	   }
	   
	   public float getNetRatio() {    
	       return netRatio;    
	   }    
	  
	   public void setNetRatio(float netRatio) {    
	       this.netRatio = netRatio;    
	   }
}
