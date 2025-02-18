package myConnection.message;

import myConnection.MyChannelManager;
import myDiscover.MyConfig;
import myDiscover.Tool;
import org.tron.p2p.connection.message.MessageType;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.protos.Discover;
import org.tron.p2p.utils.NetUtil;

public class MyStatusMessage extends MyMessage {
  private Connect.StatusMessage statusMessage;

  public MyStatusMessage(byte[] data) throws Exception {
    super(MessageType.STATUS, data);
    this.statusMessage = Connect.StatusMessage.parseFrom(data);
  }

  public MyStatusMessage() {
    super(MessageType.STATUS, null);
    Discover.Endpoint endpoint = MyConfig.getHomeNode();
    this.statusMessage = Connect.StatusMessage.newBuilder()
      .setFrom(endpoint)
      .setMaxConnections(10000)
      .setCurrentConnections(MyChannelManager.getChannels().size()+MyChannelManager.getChannelsForIncomingAttack().size())
      .setNetworkId(MyConfig.getNetwork())
      .setTimestamp(System.currentTimeMillis()).build();
    this.data = statusMessage.toByteArray();
    System.out.println("new Status Msg with RemainConn:"+this.getRemainConnections());
  }
  public MyStatusMessage(int localPort,String localNodeId){
    super(MessageType.STATUS, null);
    byte[] nodeId = Tool.hexStringToByteArray(localNodeId);
    Node myNode = new Node(nodeId,MyConfig.getLocalIp(),"",localPort,localPort);
    Discover.Endpoint endpoint = Tool.getEndpointFromNode(myNode);
    this.statusMessage = Connect.StatusMessage.newBuilder()
            .setFrom(endpoint)
            .setMaxConnections(10000)
            .setCurrentConnections(MyChannelManager.getChannels().size()+MyChannelManager.getChannelsForIncomingAttack().size())
            .setNetworkId(MyConfig.getNetwork())
            .setTimestamp(System.currentTimeMillis()).build();
    this.data = statusMessage.toByteArray();
    System.out.println("new Status Msg with RemainConn:"+this.getRemainConnections());
  }

  public int getNetworkId() {
    return this.statusMessage.getNetworkId();
  }

  public int getVersion() {
    return this.statusMessage.getVersion();
  }

  public int getRemainConnections() {
    return this.statusMessage.getMaxConnections() - this.statusMessage.getCurrentConnections();
  }

  public long getTimestamp() {
    return this.statusMessage.getTimestamp();
  }

  public Node getFrom() {
    return NetUtil.getNode(statusMessage.getFrom());
  }

  @Override
  public String toString() {
    return "[StatusMessage: " + statusMessage;
  }

  @Override
  public boolean valid() {
    return NetUtil.validNode(getFrom());
  }
}
