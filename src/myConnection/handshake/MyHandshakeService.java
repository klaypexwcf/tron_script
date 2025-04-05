package myConnection.handshake;

import myConnection.MessageProcess;
import myConnection.MyChannel;
import myConnection.MyChannelManager;
import myConnection.message.MyHelloMessage;
import myConnection.message.MyMessage;
import myDiscover.MyConfig;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.protos.Connect;

import static org.tron.p2p.connection.ChannelManager.getDisconnectReason;

public class MyHandshakeService implements MessageProcess {
    private int networkId = MyConfig.getNetwork();

    public MyHandshakeService(int networkId) {
        this.networkId =networkId;
    }

    public MyHandshakeService() {
    }

    public void startHandshake(MyChannel channel) {
        //System.out.println("start handshake for " + channel.getRemoteIp());
        sendHelloMsg(channel, DisconnectCode.NORMAL, channel.getStartTime(),channel.getLocalPort(),channel.getLocalNodeId());
    }

    @Override
    public void processMessage(MyChannel channel, MyMessage message) {
        MyHelloMessage msg = (MyHelloMessage) message;

        if (channel.isFinishHandshake()) {
            //log.warn("Close channel {}, handshake is finished", channel.getInetSocketAddress());
            channel.sendP2PDisconnectMsg(Connect.DisconnectReason.DUP_HANDSHAKE);
            channel.close("DUP_HANDSHAKE");
            return;
        }

        channel.setHelloMessage(msg);

        DisconnectCode code = MyChannelManager.processPeer(channel);
        if (code != DisconnectCode.NORMAL) {
            if (!channel.isActive()) {
                sendHelloMsg(channel, code, msg.getTimestamp(),channel.getLocalPort(),channel.getLocalNodeId());
            }
            MyChannelManager.logDisconnectReason(channel, getDisconnectReason(code));
            channel.close(getDisconnectReason(code).toString());
            return;
        }

        MyChannelManager.updateNodeId(channel, msg.getFrom().getHexId());
        if (channel.isDisconnect()) {
            return;
        }

        if (channel.isActive()) {
            if (msg.getCode() != DisconnectCode.NORMAL.getValue()
                    || (msg.getNetworkId() != networkId && msg.getVersion() != networkId)) {
                DisconnectCode disconnectCode = DisconnectCode.forNumber(msg.getCode());
                //v0.1 have version, v0.2 both have version and networkId

                MyChannelManager.logDisconnectReason(channel, getDisconnectReason(disconnectCode));
                channel.close(getDisconnectReason(disconnectCode).toString());
                return;
            }
        } else {

            if (msg.getNetworkId() != networkId) {

                sendHelloMsg(channel, DisconnectCode.DIFFERENT_VERSION, msg.getTimestamp(),channel.getLocalPort(),channel.getLocalNodeId());
                MyChannelManager.logDisconnectReason(channel, Connect.DisconnectReason.DIFFERENT_VERSION);
                channel.close("DIFFERENT_VERSION");
                return;
            }
            sendHelloMsg(channel, DisconnectCode.NORMAL, msg.getTimestamp(),channel.getLocalPort(),channel.getLocalNodeId());
        }
        channel.setFinishHandshake(true);
        channel.updateAvgLatency(System.currentTimeMillis() - channel.getStartTime());
        MyConfig.hp2pEventHandler.onConnect(channel);
    }

    protected void sendHelloMsg(MyChannel channel, DisconnectCode code, long time,int localPort,String localNodeId) {
        MyHelloMessage helloMessage = new MyHelloMessage(code, time,localPort,localNodeId);
        channel.send(helloMessage);
    }
}
