package myConnection.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import myConnection.MyChannelManager;
import myDiscover.MyConfig;
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
    }

    public void connect(String host, int port, String remoteId) {
        try {
            ChannelFuture f = connectAsync(host, port, remoteId, false, false);
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
                false);
        if (MyChannelManager.isShutdown) {
            return null;
        }
        if (channelFuture != null && future != null) {
            channelFuture.addListener(future);
        }
        return channelFuture;
    }

    public ChannelFuture connectAsync(Node node, boolean discoveryMode) {
        MDC.put("customFileName", node.getHostV4());
        log.info("entering func connectAsync1");
        ChannelFuture channelFuture =
                connectAsync(node.getInetSocketAddressV4().getAddress().getHostAddress(),
                        node.getPort(),
                        node.getId() == null ? Hex.toHexString(NetUtil.getNodeId()) : node.getHexId(),
                        discoveryMode, true);
        if (MyChannelManager.isShutdown) {
            return null;
        }
        if (channelFuture != null) {
            channelFuture.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    MDC.put("customFileName",node.getInetSocketAddressV4().getAddress().getHostAddress());
                    log.warn("Connect to peer {} fail, cause:{}", node.getInetSocketAddressV4(),
                           future.cause().getMessage());
                    future.channel().close();
                    //MyChannelManager.getPeerClient().connectAsync(node,false);
//                    if (!discoveryMode) {
//                        MyChannelManager.triggerConnect(node.getPreferInetSocketAddress());
                    //}
                    //MDC.remove("customFileName");
                }
            });
            channelFuture.channel().closeFuture().addListener(future -> {
                MDC.put("customFileName",node.getInetSocketAddressV4().getAddress().getHostAddress());
                Throwable cause = future.cause();
                log.info("channel {} closed ", node.getInetSocketAddressV4());
                if (cause != null) {
                    log.warn("Channel {} closed due to: {}", node.getInetSocketAddressV4(), cause.getMessage(), cause);
                } else {
                    log.info("Channel {} closed normally.", node.getInetSocketAddressV4());
                }
                try {
                    // 使当前线程暂停60秒，即tron默认的60秒BANTIME
                    Thread.sleep(MyConfig.DEFAULT_BAN_TIME);
                } catch (InterruptedException e) {
                    log.error("exception caught ",e);
                }
                MyChannelManager.getPeerClient().connectAsync(node,false);
                //MDC.remove("customFileName");
            });
        }
        return channelFuture;
    }

    private ChannelFuture connectAsync(String host, int port, String remoteId,
                                       boolean discoveryMode, boolean trigger) {
        log.info("entering func connectAsync2");
        MDC.put("customFileName", host);
        MyP2pChannelInitializer p2pChannelInitializer = new MyP2pChannelInitializer(remoteId,
                discoveryMode, trigger,host);

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, DefaultMessageSizeEstimator.DEFAULT);
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, MyConfig.NODE_CONNECTION_TIMEOUT);
        b.remoteAddress(host, port);
        b.handler(p2pChannelInitializer);
        b.localAddress(MyConfig.getLocalIp(),18888 + new Random().nextInt(1112));
        if (MyChannelManager.isShutdown) {
            MDC.remove("customFileName");
            return null;
        }
        //future.addListener((ChannelFutureListener) f -> MDC.remove("customFileName"));
        return b.connect();
    }
}
