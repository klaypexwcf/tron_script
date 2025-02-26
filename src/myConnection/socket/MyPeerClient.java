package myConnection.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import myConnection.MyChannelManager;
import myDiscover.MyConfig;
import myDiscover.Tool;
import nodeCrawler.NodeTestConnection.MyP2pChannelInitializerForNodeTestConnection;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.MDC;
import org.tron.p2p.discover.Node;
import org.tron.p2p.utils.NetUtil;

import java.util.Random;

@Slf4j
public class MyPeerClient {
    private EventLoopGroup workerGroup;

    public void init() {
        workerGroup = new NioEventLoopGroup(0,
                new BasicThreadFactory.Builder().namingPattern("peerClient-%d").build());
    }

    public void close() {
        workerGroup.shutdownGracefully();
        workerGroup.terminationFuture().syncUninterruptibly();
        System.out.println("peer client closed");
    }

    public void connect(String host, int port, String remoteId) {
        try {
            ChannelFuture f = connectAsync(host, port, remoteId, false, false,null);
            if (f != null) {
                f.sync().channel().closeFuture().sync();
            }
        } catch (Exception e) {
            //log.warn("PeerClient can't connect to {}:{} ({})", host, port, e.getMessage());
        }
    }

    public ChannelFuture connect(Node node, ChannelFutureListener future) {
        ChannelFuture channelFuture = connectAsync(
                node.getInetSocketAddressV4().getAddress().getHostAddress(),
                node.getPort(),
                node.getId() == null ? Hex.toHexString(NetUtil.getNodeId()) : node.getHexId(), false,
                false,null);
        if (MyChannelManager.isShutdown) {
            return null;
        }
        if (channelFuture != null && future != null) {
            channelFuture.addListener(future);
        }
        return channelFuture;
    }

    public ChannelFuture connectAsync(Node node, boolean discoveryMode,String localId) {
        MDC.put("customFileName", node.getHostV4());
        log.info("entering func connectAsync1");
        ChannelFuture channelFuture =
                connectAsync(node.getInetSocketAddressV4().getAddress().getHostAddress(),
                        node.getPort(),
                        node.getId() == null ? Hex.toHexString(NetUtil.getNodeId()) : node.getHexId(),
                        discoveryMode, true,localId);

        if (MyChannelManager.isShutdown) {
            return null;
        }
        if (channelFuture != null) {
            channelFuture.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    MDC.put("customFileName",node.getInetSocketAddressV4().getAddress().getHostAddress());
                    log.warn("Connect to peer {} fail, cause:{}", node.getInetSocketAddressV4(),
                           future.cause().getMessage());

                    String reason = future.channel().attr(MyP2pChannelInitializer.DISCONNECT_REASON_KEY).get();
                    future.channel().close();
                    //MyChannelManager.getPeerClient().connectAsync(node,false);
//                    if (!discoveryMode) {
//                        MyChannelManager.triggerConnect(node.getPreferInetSocketAddress());
                    //}
                    //MDC.remove("customFileName");
                }
            });
            channelFuture.channel().closeFuture().addListener((ChannelFutureListener) futureListener -> {
                MDC.put("customFileName",node.getInetSocketAddressV4().getAddress().getHostAddress());
                Throwable cause = futureListener.cause();
                Channel closedChannel = futureListener.channel();
                String disconnectReason = closedChannel.attr(MyP2pChannelInitializer.DISCONNECT_REASON_KEY).get();
                if (cause != null) {
                    log.warn("Channel {} closed due to: {}", node.getInetSocketAddressV4(), cause.getMessage(), cause);
                } else {
                    log.info("Channel {} closed normally.", node.getInetSocketAddressV4());
                }
                try {
                    if (disconnectReason != null) {
                        if(disconnectReason.contains("MANY")||disconnectReason.contains("RECENT")) {
                            Thread.sleep(MyConfig.DEFAULT_BAN_TIME);
                        }
                    }
                    // 使当前线程暂停60秒，即tron默认的60秒BANTIME
                } catch (InterruptedException e) {
                    log.error("exception caught ",e);
                }
                MyChannelManager.getPeerClient().connectAsync(node,false,localId);
                //MDC.remove("customFileName");
            });
        }
        return channelFuture;
    }

    private ChannelFuture connectAsync(String host, int port, String remoteId,
                                       boolean discoveryMode, boolean trigger,String localId) {
        log.info("entering func connectAsync2");
        MDC.put("customFileName", host);
        Bootstrap b = null;
        try {
            MyP2pChannelInitializer p2pChannelInitializer;
            if(localId!=null) {
                p2pChannelInitializer = getMyP2pChannelInitializer(host, remoteId, discoveryMode, trigger,localId);
            }
            else {
                p2pChannelInitializer = getMyP2pChannelInitializer(host, remoteId, discoveryMode, trigger,Tool.byteArrayToHexString(MyConfig.getLocalId()));
            }

            b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, MyConfig.NODE_CONNECTION_TIMEOUT);
            b.remoteAddress(host, port);
            b.handler(p2pChannelInitializer);
            //b.localAddress(MyConfig.getLocalIp(),);
            b.bind(18888 + new Random().nextInt(1112));
        } catch (Exception e) {
            System.out.println("exception caught " + e);
            log.error("exception caught ",e);
            throw new RuntimeException(e);
        }
        if (MyChannelManager.isShutdown) {
            MDC.remove("customFileName");
            return null;
        }
        //future.addListener((ChannelFutureListener) f -> MDC.remove("customFileName"));
        return b.connect();
    }

    private static MyP2pChannelInitializer getMyP2pChannelInitializer(String host, String remoteId, boolean discoveryMode, boolean trigger,String localId) {
        Random random = new Random();
        return new MyP2pChannelInitializer(remoteId,
                discoveryMode, trigger, host,MyConfig.getFromPort()+random.nextInt(300), localId);
    }

    public ChannelFuture testConnection(Node remoteNode,String localIp,int localPort,String localIdHex) {
        Bootstrap b = null;
        try {
            MyP2pChannelInitializerForNodeTestConnection p2pChannelInitializer=
                    new MyP2pChannelInitializerForNodeTestConnection(remoteNode.getHexId(),localPort,localIdHex, remoteNode.getHostV4());
            b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000);
            b.remoteAddress(remoteNode.getHostV4(), remoteNode.getPort());
            b.handler(p2pChannelInitializer);
            //b.localAddress(MyConfig.getLocalIp(),);
            b.bind(localPort);
            //b.connect();
        } catch (Exception e) {
            System.out.println("exception caught " + e);
            log.error("exception caught ",e);
            throw new RuntimeException(e);
        }
        //System.out.println("bootstrap configured, connect now. ip "+remoteNode.getHostV4());
        return b.connect();
    }
}
