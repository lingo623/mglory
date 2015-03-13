package com.lingo.mglory.transport.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.param.MgloryOut;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler extends ChannelInboundHandlerAdapter
{
	private static Log log = LogFactory.getLog(ClientHandler.class);
    private MgloryOut mgloryOut=new MgloryOut();
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
		//log.debug("client接收到服务器返回的消息");
		mgloryOut=(MgloryOut)msg;
    }
    public MgloryOut getMessage()
    {
        return this.mgloryOut;
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception 
    {
    	log.warn("client exception is general:"+cause);
    }

}