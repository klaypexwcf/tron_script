package nodeCrawler.NodeTestConnection;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import myConnection.MyChannel;
import myConnection.handshake.MyHandshakeService;
import myConnection.message.MyHelloMessage;
import myConnection.message.MyMessage;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.protos.Connect;

import static nodeCrawler.NodeTestConnection.MyP2pChannelInitializerForNodeTestConnection.IS_SUCCESS;
import static org.tron.p2p.connection.ChannelManager.getDisconnectReason;

@Slf4j
public class MyHandshakeServiceForNodeTestConnection extends MyHandshakeService {
    @Setter
    private String localIp;

    @Override
    protected void sendHelloMsg(MyChannel channel, DisconnectCode code, long time, int localPort, String localNodeId){
        MyHelloMessage helloMessage = new MyHelloMessage(code, time,localNodeId,localPort,localIp);
        channel.send(helloMessage);
    }

    /**
     * 收到对方发送的hello后直接断开连接，以too_many_peers的名义
     * @param channel
     * @param message
     */
    @Override
    public void processMessage (MyChannel channel, MyMessage message) {
        log.info("received Hello_msg from node {}",channel.getRemoteInetSocketAddress().getAddress().getHostAddress());
        MyHelloMessage msg = (MyHelloMessage) message;
        if (channel.isFinishHandshake()) {
            log.warn("Close channel {}, handshake is finished", channel.getRemoteInetSocketAddress().getAddress().getHostAddress());
            channel.sendP2PDisconnectMsg(Connect.DisconnectReason.DUP_HANDSHAKE);
            channel.close("DUP_HANDSHAKE");
            return;
        }
        channel.setHelloMessage(msg);
        DisconnectCode code = DisconnectCode.TOO_MANY_PEERS;
        //收到hello就算成功
        channel.getCtx().channel().attr(IS_SUCCESS).set(true);

        channel.close(getDisconnectReason(code).toString());
        return;
    }
}
