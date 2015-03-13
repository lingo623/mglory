package com.lingo.mglory.route;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;  
import java.util.SortedMap;  
import java.util.TreeMap;  
  
public class MgloryHash<T> {  
 
   private final int numberOfReplicas;// 每个机器节点关联的虚拟节点个数  
   private final SortedMap<Long, T> circle = new TreeMap<Long, T>();// 环形虚拟节点  
  
    /** 
     *  
     * @param numberOfReplicas 
     *            每个机器节点关联的虚拟节点个数 
     * @param nodes 
     *            真实机器节点 
     */  
    public MgloryHash(int numberOfReplicas, Collection<T> nodes) {   
        this.numberOfReplicas = numberOfReplicas;  
        for (T node : nodes) {  
            add(node);  
        }  
    }  
    public Long hash(String key) {  
        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());  
        int seed = 0x1234ABCD;
  
        ByteOrder byteOrder = buf.order();  
        buf.order(ByteOrder.LITTLE_ENDIAN);  
  
        long m = 0xc6a4a7935bd1e995L;  
        int r = 47;  
  
        long h = seed ^ (buf.remaining() * m);  
  
        long k;  
        while (buf.remaining() >= 8) {  
            k = buf.getLong();  
  
            k *= m;  
            k ^= k >>> r;  
            k *= m;  
  
            h ^= k;  
            h *= m;  
        }  
  
        if (buf.remaining() > 0) {  
            ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);            
            finish.put(buf).rewind();  
            h ^= finish.getLong();  
            h *= m;  
        }  
  
        h ^= h >>> r;  
        h *= m;  
        h ^= h >>> r;  
  
        buf.order(byteOrder);  
        return h;  
    }
    /** 
     * 增加真实机器节点 
     *  
     * @param node 
     */  
    public void add(T node) {  
        for (int i = 0; i < this.numberOfReplicas; i++) {  
            circle.put(hash(node.toString() + i), node);  
        }  
    }  
  
    /** 
     * 删除真实机器节点 
     *  
     * @param node 
     */  
    public void remove(T node) {  
        for (int i = 0; i < this.numberOfReplicas; i++) {  
            circle.remove(hash(node.toString() + i));  
        }  
    }  
  
    /** 
     * 取得真实机器节点 
     *  
     * @param key 
     * @return 
     */  
    public T get(String key) {  
        if (circle.isEmpty()) {  
            return null;  
        }  
  
        long hash =hash(key);  
        if (!circle.containsKey(hash)) {  
            SortedMap<Long, T> tailMap = circle.tailMap(hash);// 沿环的顺时针找到一个虚拟节点  
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();  
        }  
  
        return circle.get(hash); // 返回该虚拟节点对应的真实机器节点的信息  
    }  
}  
