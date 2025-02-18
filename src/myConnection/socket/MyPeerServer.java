package myConnection.socket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import myDiscover.MyConfig;
import myDiscover.table.NodeIdTable;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class MyPeerServer  {
    private Map<Integer, ChannelFuture> channelFutureList = new ConcurrentHashMap<>();
    private Map<Integer,EventLoopGroup> bossGroupMap = new ConcurrentHashMap<>();
    private Map<Integer,EventLoopGroup> workerGroupMap = new ConcurrentHashMap<>();
    private boolean listening;

    public  void init(int startPort, int Range, NodeIdTable nodeIdTable){
        for(int i = startPort; i<startPort+Range; i++){
            int finalI = i;
            int nodeSeq =i-startPort;
            new Thread(() -> start(finalI, nodeIdTable.getNodeIdFromSeq(nodeSeq).getHexId()),"PeerServer_"+finalI).start();
        }
    }
    public void close(){
        if (listening && channelFutureList != null ) {
            try {
                for(ChannelFuture channelFuture : channelFutureList.values()) {
                    if(channelFuture.channel().isOpen()) {
                        System.out.println("Closing one TCP server...");
                        channelFuture.channel().close().sync();
                    }
                }
            } catch (Exception e) {
                System.out.println("Closing TCP server failed."+e);
            }
        }
    }
    public void start(int localPort,String localNodeId) {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1,
                new BasicThreadFactory.Builder().namingPattern("peerBoss").build());
        //if threads = 0, it is number of core * 2
        EventLoopGroup workerGroup = new NioEventLoopGroup(0,
                new BasicThreadFactory.Builder().namingPattern("peerWorker-%d").build());
        MyP2pChannelInitializer p2pChannelInitializer = new MyP2pChannelInitializer("", false, true,"1111test",localPort,localNodeId);
        try {
            ServerBootstrap b = new ServerBootstrap();

            b.group(bossGroup, workerGroup);
            b.channel(NioServerSocketChannel.class);

            b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, MyConfig.NODE_CONNECTION_TIMEOUT);

            b.handler(new LoggingHandler());
            b.childHandler(p2pChannelInitializer);

            // Start the client.
            System.out.println("TCP listener started, bind port "+localPort);


            ChannelFuture channelFuture = b.bind(localPort).sync();
            channelFutureList.put(localPort, channelFuture);
            bossGroupMap.put(localPort, bossGroup);
            workerGroupMap.put(localPort, workerGroup);

            listening = true;

            new Thread(() -> {
                try {
                    channelFuture.channel().closeFuture().sync();
                    System.out.println("Channel on port " + localPort + " is closed.");
                    channelFutureList.remove(localPort);
                    bossGroupMap.remove(localPort);
                    workerGroupMap.remove(localPort);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }).start();
        } catch (Exception e) {
            System.out.printf("Start TCP server failed"+ e);
            e.printStackTrace();
        }
    }
}
