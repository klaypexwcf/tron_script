package myConnection.socket;

import io.netty.util.AttributeKey;
import myConnection.MyChannel;
import myConnection.MyChannelManager;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
public class MyP2pChannelInitializer extends ChannelInitializer<NioSocketChannel> {
    private final String remoteId;

    private boolean peerDiscoveryMode = false; //only be true when channel is activated by detect service

    private boolean trigger = true;

    private final String remoteAddress;
    private final int localPort;
    private final String localNodeId;
    private String disconnectReason="";
    public static final AttributeKey<String> DISCONNECT_REASON_KEY = AttributeKey.valueOf("disconnectReason");
    public MyP2pChannelInitializer(String remoteId, boolean peerDiscoveryMode, boolean trigger,String justAnUnusedVariable,int localPort,String localNodeId) {
        this.localNodeId = localNodeId;
        this.localPort = localPort;
        this.remoteId = remoteId;
        this.peerDiscoveryMode = peerDiscoveryMode;
        this.trigger = trigger;
        this.remoteAddress = justAnUnusedVariable;
    }

    @Override
    public void initChannel(NioSocketChannel ch) {
        try {
            MDC.put("customFileName", remoteAddress);
            System.out.println(MDC.get("customFileName"));
            final MyChannel channel = new MyChannel();
            channel.init(ch.pipeline(), remoteId, peerDiscoveryMode,localPort,localNodeId);

            // limit the size of receiving buffer to 1024
            ch.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(256 * 1024));
            ch.config().setOption(ChannelOption.SO_RCVBUF, 256 * 1024);
            ch.config().setOption(ChannelOption.SO_BACKLOG, 1024);
            ch.attr(DISCONNECT_REASON_KEY).set(disconnectReason);
            // be aware of channel closing
            ch.closeFuture().addListener((ChannelFutureListener) future -> {
                channel.setDisconnect(true);
                disconnectReason= channel.getDisconnectReason();
                ch.attr(DISCONNECT_REASON_KEY).set(disconnectReason);
                if (channel.isDiscoveryMode()) {
                    //MyChannelManager.getNodeDetectService().notifyDisconnect(channel);
                } else {
                    try {
                        log.info("Close channel:{}", channel.getRemoteInetSocketAddress());
                        MyChannelManager.notifyDisconnect(channel);
                    } finally {
                        if (channel.getRemoteInetSocketAddress() != null && channel.isActive() && trigger) {
                            //MyChannelManager.triggerConnect(channel.getInetSocketAddress());
                        }
                    }
                }
            });

        } catch (Exception e) {
            System.out.println("Unexpected initChannel error"+e);
            log.error("Unexpected initChannel error", e);
        }finally {
            //MDC.remove("customFileName");
        }
    }
}
