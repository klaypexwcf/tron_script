package myConnection;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import myConnection.higher.MyHMessage.MyHDisconnectMessage;
import myConnection.higher.MyHMessage.MyHHelloMessage;
import myConnection.message.*;
import myConnection.socket.MyMessageHandler;
import myConnection.socket.MyP2pProtobufVariant32FrameDecoder;
import nodeCrawler.NodeCrawler;
import nodeCrawler.NodeTestConnection.MyHandshakeServiceForNodeTestConnection;
import nodeCrawler.NodeTestConnection.MyMessageHandlerForNodeTestConnection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.tron.p2p.connection.business.upgrade.UpgradeController;
import org.tron.p2p.discover.Node;
import org.tron.p2p.exception.P2pException;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.stats.TrafficStats;
import org.tron.p2p.utils.ByteArray;
import org.tron.protos.Protocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static myConnection.socket.MyP2pChannelInitializer.DISCONNECT_REASON_KEY;

@Slf4j()
public class MyChannel {
    public volatile boolean waitForPong = false;
    public volatile long pingSent = System.currentTimeMillis();

    @Getter
    private MyHelloMessage helloMessage;
    @Setter
    @Getter
    public MyHHelloMessage hHelloMessage;
    @Getter
    private Node remoteNode;
    @Getter
    private int version;
    @Getter
    protected int localPort;
    @Getter
    protected String localNodeId;
    @Getter
    private ChannelHandlerContext ctx;
    @Getter
    private InetSocketAddress remoteInetSocketAddress;
    @Getter
    @Setter
    private String disconnectReason="";
    @Getter
    private InetAddress remoteInetAddress;
    @Getter
    private volatile long disconnectTime;
    @Getter
    @Setter
    private volatile boolean isDisconnect = false;
    @Getter
    @Setter
    private long lastSendTime = System.currentTimeMillis();
    @Getter
    private final long startTime = System.currentTimeMillis();
    @Getter
    protected boolean isActive = false;
    @Getter
    private boolean isTrustPeer;
    @Getter
    @Setter
    private volatile boolean finishHandshake;
    @Getter
    @Setter
    private String remoteNodeId;
    @Setter
    @Getter
    private boolean discoveryMode;
    @Getter
    private long avgLatency;
    private long count;
    @Getter
    protected String remoteIp;

    public void init(ChannelPipeline pipeline, String remoteNodeId, boolean discoveryMode, int localPort, String localNodeId) {
        this.localNodeId = localNodeId;
        this.localPort = localPort;
        this.discoveryMode = discoveryMode;
        this.remoteNodeId = remoteNodeId;
        this.isActive = StringUtils.isNotEmpty(remoteNodeId);
        MyMessageHandler messageHandler = new MyMessageHandler(this);
        pipeline.addLast("readTimeoutHandler", new ReadTimeoutHandler(60, TimeUnit.SECONDS));
        pipeline.addLast(TrafficStats.tcp);
        pipeline.addLast("protoPrepend", new ProtobufVarint32LengthFieldPrepender());
        pipeline.addLast("protoDecode", new MyP2pProtobufVariant32FrameDecoder(this));
        pipeline.addLast("messageHandler", messageHandler);
    }
    public void initForTestConnection(ChannelPipeline pipeline, String remoteNodeId, boolean discoveryMode, int localPort, String localNodeId,String remoteIp) {
        this.localNodeId = localNodeId;
        this.localPort = localPort;
        this.discoveryMode = discoveryMode;
        this.remoteNodeId = remoteNodeId;
        this.isActive = StringUtils.isNotEmpty(remoteNodeId);
        this.remoteIp = remoteIp;
        MyHandshakeServiceForNodeTestConnection myHandshakeServiceForNodeTestConnection =new MyHandshakeServiceForNodeTestConnection();
        myHandshakeServiceForNodeTestConnection.setLocalIp(NodeCrawler.localIp);
        MyMessageHandlerForNodeTestConnection messageHandler = new MyMessageHandlerForNodeTestConnection(this, myHandshakeServiceForNodeTestConnection);
        pipeline.addLast("readTimeoutHandler", new ReadTimeoutHandler(60, TimeUnit.SECONDS));
        pipeline.addLast(TrafficStats.tcp);
        pipeline.addLast("protoPrepend", new ProtobufVarint32LengthFieldPrepender());
        pipeline.addLast("protoDecode", new MyP2pProtobufVariant32FrameDecoder(this));
        pipeline.addLast("messageHandler", messageHandler);
    }

    public void processException(Throwable throwable) {
        Throwable baseThrowable = throwable;
        try {
            baseThrowable = Throwables.getRootCause(baseThrowable);
        } catch (IllegalArgumentException e) {
            baseThrowable = e.getCause();
            log.warn("Loop in causal chain detected");
        }
        SocketAddress address = ctx.channel().remoteAddress();
        if (throwable instanceof ReadTimeoutException
                || throwable instanceof IOException
                || throwable instanceof CorruptedFrameException) {
            log.warn("Close peer {}, reason: {}", address, throwable.getMessage());
        } else if (baseThrowable instanceof P2pException) {
            log.warn("Close peer {}, type: ({}), info: {}",
                    address, ((P2pException) baseThrowable).getType(), baseThrowable.getMessage());
        } else {
            log.error("Close peer {}, exception caught", address, throwable);
        }
        close(throwable.getMessage());
    }

    public void setHelloMessage(MyHelloMessage helloMessage) {
        this.helloMessage = helloMessage;
        this.remoteNode = helloMessage.getFrom();
        this.remoteNodeId = remoteNode.getHexId(); //update node id from handshake
        this.version = helloMessage.getVersion();
    }

    public void setChannelHandlerContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.remoteInetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        this.remoteInetAddress = remoteInetSocketAddress.getAddress();
        this.isTrustPeer =false;
    }

    public void close(long banTime,String reason) {
        this.isDisconnect = true;
        this.disconnectTime = System.currentTimeMillis();
        disconnectReason =reason;
        ctx.attr(DISCONNECT_REASON_KEY).set(disconnectReason);
        //System.out.println("channel "+ localPort+ " closed because "+reason);
        //MyChannelManager.banNode(this.remoteInetAddress, banTime);
        ctx.close();
    }

    public void close(String reason) {
        close(1L,reason);
    }
    public void sendStatusMsg(){
        MyStatusMessage statusMessage = new MyStatusMessage(localPort,localNodeId);
        send(statusMessage);
    }
    public void sendP2PDisconnectMsg(Connect.DisconnectReason disconnectReason){
        send(new MyP2pDisconnectMessage(disconnectReason));
    }
    public void sendPingMsg(){
        send(new MyPingMessage());
    }
    public void sendPongMsg(){
        send(new MyPongMessage());
    }
    public void sendHDisconnectMsg(Protocol.ReasonCode reasonCode){
        send(new MyHDisconnectMessage(reasonCode).getSendBytes());
    }
    public void sendHHelloMsg(int localPort,String localNodeId){
        MyHHelloMessage myHHelloMessage= new MyHHelloMessage(localPort,localNodeId);
        send(myHHelloMessage.getSendBytes());
        setHHelloMessage(myHHelloMessage);
    }
    public void send(MyMessage message) {
        MDC.put("customFileName", remoteInetAddress.getHostAddress());
        if (message.needToLog()) {
            log.info("Send message to channel {}, {}", remoteInetSocketAddress, message);
        } else {
            log.debug("Send message to channel {}, {}", remoteInetSocketAddress, message);
        }
        //MDC.remove("customFileName");
        send(message.getSendData());
    }

    public void send(byte[] data) {
        try {
            byte type = data[0];
            if (isDisconnect) {
                log.warn("Send to {} failed as channel has closed, message-type:{} ",
                        ctx.channel().remoteAddress(), type);
                return;
            }

            if (finishHandshake) {
                data = UpgradeController.codeSendData(version, data);
            }

            ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
            ctx.writeAndFlush(byteBuf).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess() && !isDisconnect) {
                    log.warn("Send to {} failed, message-type:{}, cause:{}",
                            ctx.channel().remoteAddress(), ByteArray.byte2int(type),
                            future.cause().getMessage());
                }
            });
            setLastSendTime(System.currentTimeMillis());
        } catch (Exception e) {
            log.warn("Send message to {} failed, {}", remoteInetSocketAddress, e.getMessage());
            ctx.channel().close();
        }
    }

    public void updateAvgLatency(long latency) {
        long total = this.avgLatency * this.count;
        this.count++;
        this.avgLatency = (total + latency) / this.count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MyChannel channel = (MyChannel) o;
        return Objects.equals(remoteInetSocketAddress, channel.remoteInetSocketAddress);
    }

    @Override
    public int hashCode() {
        return remoteInetSocketAddress.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s | %s", remoteInetSocketAddress,
                StringUtils.isEmpty(remoteNodeId) ? "<null>" : remoteNodeId);
    }
}
