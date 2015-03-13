package com.lingo.mglory.route;

public class Node<T> {  
 
    private String resource;   
  
    public Node(String resource) {  
        this.resource = resource;  
    }  
  
    public String getResource() {  
        return resource;  
    }  
  
    public void setResource(String resource) {  
        this.resource = resource; 
    }
    /** 
     * 复写toString方法，使用节点resource当做hash的KEY 
     */  
    @Override  
    public String toString() {  
        return resource;  
    }  
  
}  
