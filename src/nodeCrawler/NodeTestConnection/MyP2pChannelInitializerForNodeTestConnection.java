package nodeCrawler.NodeTestConnection;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import myConnection.MyChannel;

@Slf4j(topic = "nodeOnlineUpdater")
public class MyP2pChannelInitializerForNodeTestConnection  extends ChannelInitializer<NioSocketChannel> {
    public static final AttributeKey<byte[]> NEW_NODE_ID = AttributeKey.valueOf("newNodeId");
    private final String remoteId;
    private final int localPort;
    private final String localNodeId;
    private String disconnectReason="";
    private final String remoteIp;
    public static final AttributeKey<String> DISCONNECT_REASON_KEY = AttributeKey.valueOf("disconnectReason");

    public MyP2pChannelInitializerForNodeTestConnection(String remoteId, int localPort, String localNodeId,String remoteIp) {
        this.remoteId = remoteId;
        this.localPort = localPort;
        this.localNodeId = localNodeId;
        this.remoteIp = remoteIp;
    }
    @Override
    protected void initChannel(NioSocketChannel ch) throws Exception {
        final MyChannel channel = new MyChannel();
        channel.initForTestConnection(ch.pipeline(), remoteId, false,localPort,localNodeId,remoteIp);
        //System.out.println("channel initialized");
        ch.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(256 * 1024));
        ch.config().setOption(ChannelOption.SO_RCVBUF, 256 * 1024);
        ch.config().setOption(ChannelOption.SO_BACKLOG, 1024);
        ch.attr(DISCONNECT_REASON_KEY).set(disconnectReason);
        ch.attr(NEW_NODE_ID).set(null);
        ch.closeFuture().addListener((ChannelFutureListener) future -> {
            channel.setDisconnect(true);
            disconnectReason= channel.getDisconnectReason();
            ch.attr(DISCONNECT_REASON_KEY).set(disconnectReason);
        });
    }
}
