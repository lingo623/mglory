package com.lingo.mglory.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils {
	public static void setRmiPara()
	{
		Properties config= PropertiesUtil.getInstance().getConfig("config.properties");
		//设置调用超时时间
		if (config.getProperty("clientTimeOut")!=null && System.getProperty("sun.rmi.transport.proxy.connectTimeout")!=null)
		{
			String timeOut=config.getProperty("clientTimeOut");
			System.setProperty("sun.rmi.transport.proxy.connectTimeout", timeOut);
			System.setProperty("sun.rmi.transport.tcp.responseTimeout", timeOut);
		}
	}
	public static <T> T newInstance(Class<T> clazz) {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String className) {
		try {
			Class<T> clazz=(Class<T>) Class.forName(className);
			return clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public static<T> Map<String,Object> transMap(Map<String,T> map)
	{
		Map<String,Object> result=new HashMap<String,Object>();
		for (String key:map.keySet())
		{
			result.put(key, map.get(key));
		}
		return result;
	}
	public static Map<String,String> transToStrMap(Map<String,Object> map)
	{
		Map<String,String> result=new HashMap<String,String>();
		for (String key:map.keySet())
		{
			result.put(key, map.get(key)==null?"":map.get(key).toString());
		}
		return result;
	}
	public static Map<String,String> transStrToMap(String str)
	{
		
		Map<String,String> map=new HashMap<String,String>();
		if (str!=null)
		{
			String[] kvs=str.split("!,!");
			for (String kv:kvs)
			{
				String k=kv.split("!=!")[0];
				String v=kv.split("!=!").length==2?kv.split("!=!")[1]:null;
				map.put(k, v);
			}
		}
		return map;
		
	}
	@SuppressWarnings("unchecked")
	public static String transMapToStr(Object obj){
		if (obj instanceof Map)
		{
			Map<String,String> map=(Map)obj;
			StringBuffer sb = new StringBuffer();
			for(String key:map.keySet()) 
			{ 
				sb.append(key+"!=!"+map.get(key)+"!,!");
			} 
			return sb.toString();
		}
		else
		{
			return obj==null?null:obj.toString();
		}
		
	}
	/**
	 * 获取序列
	 * @param rLength 生成随机数的长度
	 * @return 时间数字(System.nanoTime()的后6位)+rLength位随机数
	 */
	public static String getSeqId() {
		int rLength=10;
		StringBuilder b = new StringBuilder("");
		String t = String.valueOf(System.nanoTime());
		if (t.length()>6)
		{
			t=t.substring(t.length()-6,t.length());
		}
	    String r=getFixLenthString(rLength-4);
		b.append(getSysdate("MMdd")).append(t).append(r);
		return b.toString();
	}
   /**
    * 返回长度为【strLength】的随机数，在后面补0
    */
   public static String getFixLenthString(int strLength) {
         double d=Double.parseDouble(String.valueOf(Math.pow(10, strLength)));
         long r = (long) Math.floor(Math.random() *d);
         String random=String.valueOf(r);
         for (int i=0;i<strLength-String.valueOf(r).length();i++)
         {
        	 random=random+"0";
         }
         return random.equals("0")?"":random;
    }
   /**
	 * 获取系统时间
	 * @param format 14位:yyyyMMddHHmmss,19位：yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String getSysdate(String format)
	{
		Date now = new Date(); 
		//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(now); 
	}
}
