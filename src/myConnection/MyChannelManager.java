package myConnection;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.Getter;
import myConnection.handshake.MyHandshakeService;
import myConnection.keepalive.MyKeepAliveService;
import myConnection.message.MyMessage;
import myConnection.message.MyP2pDisconnectMessage;
import myConnection.socket.MyPeerClient;
import myConnection.socket.MyPeerServer;
import myDiscover.MyConfig;
import myDiscover.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.discover.Node;
import org.tron.p2p.exception.P2pException;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.utils.ByteArray;
import org.tron.p2p.utils.NetUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class MyChannelManager {
    private static final Logger log= LoggerFactory.getLogger(MyChannelManager.class);

    @Getter
    private static MyHandshakeService handshakeService;
    @Getter
    private static MyPeerClient peerClient;

    private static MyPeerServer peerServer;

    private static MyKeepAliveService keepAliveService;

    private static MyNodeDetectService nodeDetectService;

    @Getter
    private static final Map<InetSocketAddress, MyChannel> channels = new ConcurrentHashMap<>();

    private static boolean isInit = false;

    public static volatile boolean isShutdown = false;


    public static void main(String[] args) {
        String localIdString;
        String remoteIp = "";
        String localIp = NetUtil.getExternalIpV4();
        for (int i=0;i<args.length;i++) {
            if (args[i].equals("--localIp")) {
                localIp = args[++i];
            }
            if (args[i].equals("--remoteIp")) {
                remoteIp = args[++i];
            }
        }

        MDC.put("customFileName",remoteIp);
        log.info("Starting....");
        MyChannelManager.init();

        System.out.println("NetUtil detect localIp"+localIp);

        byte[] randomId = Tool.generateRandomNodeId();
        MyConfig.init(randomId);
        MyConfig.setLocalIp(localIp);
        System.out.println("localIp:"+localIp+" remoteIp:"+remoteIp);

        log.info("MainClass: Starting task; for {}", remoteIp);
        ChannelFuture channelFuture = peerClient.connectAsync(MyConfig.wrapNode(remoteIp), false);
        //MDC.remove("customFileName");

//        for (int i = 0; i < args.length; i++) {
//            if ("--localId".equals(args[i])) {
//                if (i + 1 < args.length) {  // 确保端口值存在
//                    try {
//                        localIdString = args[i + 1];
//                        MyConfig.init(localIdString);
//                    } catch (NumberFormatException e) {
//                        System.err.println("Invalid id: " + args[i + 1]);
//                        System.exit(1);
//                    }
//                }
//            }
//            if("--remoteIp".equals(args[i])) {
//                if (i + 1 < args.length) {  // 确保端口值存在
//                    try {
//                        remoteIp = args[i + 1];
//                        MyConfig.setToIp(remoteIp);
//
//                    } catch (NumberFormatException e) {
//                        System.err.println("Invalid ip: " + args[i + 1]);
//                        System.exit(1);
//                    }
//                }
//            }
//        }

//        ChannelFuture channelFuture = peerClient.connectAsync(MyConfig.getTo(), false);

    }

    public static void init(){
        isInit=true;
        handshakeService=new MyHandshakeService();
        peerClient = new MyPeerClient();
        peerServer = new MyPeerServer();
        keepAliveService = new MyKeepAliveService();
        //peerServer.init();
        peerClient.init();
        keepAliveService.init();

    }

    public static void connect(InetSocketAddress address) {
        peerClient.connect(address.getAddress().getHostAddress(), address.getPort(),
                ByteArray.toHexString(MyConfig.getLocalId()));
    }

    public static ChannelFuture connect(Node node, ChannelFutureListener future) {
        return peerClient.connect(node, future);
    }
    public static void notifyDisconnect(MyChannel channel) {
        if (channel.getInetSocketAddress() == null) {
            return;
        }
        channels.remove(channel.getInetSocketAddress());
        MyConfig.handlerList.forEach(h -> h.onDisconnect(channel));
        InetAddress inetAddress = channel.getInetAddress();

    }
    public static synchronized DisconnectCode processPeer(MyChannel channel) {


        channels.put(channel.getInetSocketAddress(), channel);


        return DisconnectCode.NORMAL;
    }
    public static void close() {
        if (!isInit || isShutdown) {
            return;
        }
        isShutdown = true;

        keepAliveService.close();
        peerServer.close();
        peerClient.close();

    }
    public static void processMessage(MyChannel channel, byte[] data) throws P2pException {
        MDC.put("customFileName",channel.getInetSocketAddress().getAddress().getHostAddress());
        if (data == null || data.length == 0) {
            throw new P2pException(P2pException.TypeEnum.EMPTY_MESSAGE, "");
        }
        if (data[0] >= 0) {
            handMessage(channel, data);
            return;
        }

        MyMessage message = MyMessage.parse(data);

        System.out.println("receive msg from channel "+channel.getInetSocketAddress()+"type: "+message);


        switch (message.getType()) {
            case KEEP_ALIVE_PING:

                log.info("received KEEP_ALIVE_PING from {}",channel.getInetSocketAddress());
                keepAliveService.processMessage(channel, message);
                break;
            case KEEP_ALIVE_PONG:
                log.info("received KEEP_ALIVE_PONG from {}",channel.getInetSocketAddress());
                keepAliveService.processMessage(channel, message);
                break;
            case HANDSHAKE_HELLO:
                log.info("received HANDSHAKE_HELLO from {}, {}",channel.getInetSocketAddress(), message);
                handshakeService.processMessage(channel, message);
                break;
            case STATUS:
                log.info("received STATUS from {}, {}",channel.getInetSocketAddress(),message);
                //nodeDetectService.processMessage(channel, message);
                break;
            case DISCONNECT:
                log.info("received DISCONNECT from {}, reason: {}",channel.getInetSocketAddress(), message);
                channel.close();
                break;
            default:
                throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, "type:" + data[0]);
        }
    }
    private static void handMessage(MyChannel channel, byte[] data) throws P2pException {

        if (channel.isDiscoveryMode()) {
            channel.send(new MyP2pDisconnectMessage(Connect.DisconnectReason.DISCOVER_MODE));
            channel.getCtx().close();
            return;
        }

        if (!channel.isFinishHandshake()) {
            channel.setFinishHandshake(true);
            DisconnectCode code = processPeer(channel);
            if (!DisconnectCode.NORMAL.equals(code)) {
                Connect.DisconnectReason disconnectReason = getDisconnectReason(code);
                channel.send(new MyP2pDisconnectMessage(disconnectReason));
                channel.getCtx().close();
                return;
            }
            MyConfig.hp2pEventHandler.onConnect(channel);
        }

        MyConfig.hp2pEventHandler.onMessage(channel, data);
    }
    public static Connect.DisconnectReason getDisconnectReason(DisconnectCode code) {
        Connect.DisconnectReason disconnectReason;
        switch (code) {
            case DIFFERENT_VERSION:
                disconnectReason = Connect.DisconnectReason.DIFFERENT_VERSION;
                break;
            case TIME_BANNED:
                disconnectReason = Connect.DisconnectReason.RECENT_DISCONNECT;
                break;
            case DUPLICATE_PEER:
                disconnectReason = Connect.DisconnectReason.DUPLICATE_PEER;
                break;
            case TOO_MANY_PEERS:
                disconnectReason = Connect.DisconnectReason.TOO_MANY_PEERS;
                break;
            case MAX_CONNECTION_WITH_SAME_IP:
                disconnectReason = Connect.DisconnectReason.TOO_MANY_PEERS_WITH_SAME_IP;
                break;
            default: {
                disconnectReason = Connect.DisconnectReason.UNKNOWN;
            }
        }
        return disconnectReason;
    }
    public static synchronized void updateNodeId(MyChannel channel, String nodeId) {
        channel.setNodeId(nodeId);
//        if (nodeId.equals(Hex.toHexString(MyConfig.getLocalId()))) {
//            //log.warn("Channel {} is myself", channel.getInetSocketAddress());
//            channel.send(new MyP2pDisconnectMessage(Connect.DisconnectReason.DUPLICATE_PEER));
//            channel.close();
//            return;
//        }

//        List<MyChannel> list = new ArrayList<>();
//        channels.values().forEach(c -> {
//            if (nodeId.equals(c.getNodeId())) {
//                list.add(c);
//            }
//        });
//        if (list.size() <= 1) {
//            return;
//        }
//        Channel c1 = list.get(0);
//        Channel c2 = list.get(1);
//        if (c1.getStartTime() > c2.getStartTime()) {
//            log.info("Close channel {}, other channel {} is earlier", c1, c2);
//            c1.send(new P2pDisconnectMessage(Connect.DisconnectReason.DUPLICATE_PEER));
//            c1.close();
//        } else {
//            log.info("Close channel {}, other channel {} is earlier", c2, c1);
//            c2.send(new P2pDisconnectMessage(Connect.DisconnectReason.DUPLICATE_PEER));
//            c2.close();
//        }
    }

    //    public static void triggerConnect(InetSocketAddress address) {
//        connPoolService.triggerConnect(address);
//    }
    public static void logDisconnectReason(MyChannel channel, Connect.DisconnectReason reason) {
        log.info("Try to close channel: {}, reason: {}", channel.getInetSocketAddress(), reason.name());
}

//    public static void triggerConnect(InetSocketAddress preferInetSocketAddress) {
//        // 开始重新连接，创建新的 Channel 并尝试连接
//        try {
//            ChannelFuture future = bootstrap.connect(address);
//            future.addListener((ChannelFutureListener) connectFuture -> {
//                if (connectFuture.isSuccess()) {
//                    log.info("Reconnected to: {}", address);
//                } else {
//                    log.warn("Reconnect to {} failed, retrying...", address);
//                    // 重新连接失败后，可以加入延迟，再次重试
//                    // 比如每隔几秒钟重试一次
//                    scheduleReconnect(address);
//                }
//            });
//        } catch (Exception e) {
//            log.error("Exception occurred during reconnect: {}", e.getMessage());
//        }
//    }
}
