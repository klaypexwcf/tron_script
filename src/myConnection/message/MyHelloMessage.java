package myConnection.message;

import myDiscover.MyConfig;
import myDiscover.Tool;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.connection.message.MessageType;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.protos.Discover;
import org.tron.p2p.utils.NetUtil;

public class MyHelloMessage extends MyMessage {
    private Connect.HelloMessage helloMessage;

    public MyHelloMessage(byte[] data) throws Exception {
        super(MessageType.HANDSHAKE_HELLO, data);
        this.helloMessage = Connect.HelloMessage.parseFrom(data);
    }

    public MyHelloMessage(DisconnectCode code, long time) {
        super(MessageType.HANDSHAKE_HELLO, null);
        Discover.Endpoint endpoint = MyConfig.getHomeNode();
        this.helloMessage = Connect.HelloMessage.newBuilder()
                .setFrom(endpoint)
                .setNetworkId(MyConfig.getNetwork())
                .setCode(code.getValue())
                .setVersion(MyConfig.version)
                .setTimestamp(time).build();
        this.data = helloMessage.toByteArray();
    }
    public MyHelloMessage(DisconnectCode code, long time,int localPort,String localNodeId) {
        super(MessageType.HANDSHAKE_HELLO, null);
        byte[] nodeId = Tool.hexStringToByteArray(localNodeId);
        Node myNode = new Node(nodeId,MyConfig.getLocalIp(),"",localPort,localPort);
        Discover.Endpoint endpoint = Tool.getEndpointFromNode(myNode);
        this.helloMessage = Connect.HelloMessage.newBuilder()
                .setFrom(endpoint)
                .setNetworkId(MyConfig.getNetwork())
                .setCode(code.getValue())
                .setVersion(MyConfig.version)
                .setTimestamp(time).build();
        this.data = helloMessage.toByteArray();
    }
    public MyHelloMessage(DisconnectCode code, long time,String localNodeId,int localPort,String localIp) {
        super(MessageType.HANDSHAKE_HELLO, null);
        byte[] nodeId = Tool.hexStringToByteArray(localNodeId);
        Node myNode = new Node(nodeId,localIp,"",localPort,localPort);
        Discover.Endpoint endpoint = Tool.getEndpointFromNode(myNode);
        this.helloMessage = Connect.HelloMessage.newBuilder()
                .setFrom(endpoint)
                .setNetworkId(11111)
                .setCode(code.getValue())
                .setVersion(1)
                .setTimestamp(time).build();
        this.data = helloMessage.toByteArray();
    }

    public int getNetworkId() {
        return this.helloMessage.getNetworkId();
    }

    public int getVersion() {
        return this.helloMessage.getVersion();
    }

    public int getCode() {
        return this.helloMessage.getCode();
    }

    public long getTimestamp() {
        return this.helloMessage.getTimestamp();
    }

    public Node getFrom() {
        return NetUtil.getNode(helloMessage.getFrom());
    }

    @Override
    public String toString() {
        return "[HelloMessage: " + helloMessage;
    }

    @Override
    public boolean valid() {
        return NetUtil.validNode(getFrom());
    }
}
