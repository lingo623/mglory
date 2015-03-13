package com.lingo.mglory.transport.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lingo.mglory.transport.handler.ServerHandler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
public class Server {
	private static Log log = LogFactory.getLog(Server.class);
    protected static final int BIZGROUPSIZE = Runtime.getRuntime().availableProcessors() * 2;
    //protected static final int BIZTHREADSIZE = 4;
    private static final EventLoopGroup bossGroup = new NioEventLoopGroup(BIZGROUPSIZE);
    //private static final EventLoopGroup workerGroup = new NioEventLoopGroup(BIZTHREADSIZE);
    private static final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private Server()
    {
    	
    }
    public static void start(String ip,int port) throws Exception {
    	ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup);
        b.channel(NioServerSocketChannel.class);
        b.option(ChannelOption.SO_BACKLOG, 1000000);
        b.childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline pipeline = ch.pipeline();
                  pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                  pipeline.addLast(new LengthFieldPrepender(4));
                  pipeline.addLast("encode", new ObjectEncoder());  
                  pipeline.addLast("decode", new ObjectDecoder(ClassResolvers.weakCachingConcurrentResolver(null))); 
                  pipeline.addLast(new ServerHandler());
              }
          });
         b.bind(ip, port).sync();
         log.info("服务器已启动");
    }
    public static void shutdown() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("开始启动服务器...");
        start("localhost",9990);
        System.out.println("11111111111111111111111");
        //shutdown();
    }

}
