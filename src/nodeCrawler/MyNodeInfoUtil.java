package nodeCrawler;

import myDiscover.table.NodeId;

public class MyNodeInfoUtil {
    public String ipv4;
    public String ipv6;
    public int tcpPort;
    public int udpPort;
    public NodeId nodeId;
    //TODO：添加更多信息字段


    public MyNodeInfoUtil(String ipv4, int udpPort, NodeId nodeId) {
        this.ipv4 = ipv4;
        this.udpPort = udpPort;
        this.nodeId = nodeId;
    }

    public MyNodeInfoUtil(String ipv4, String ipv6, int tcpPort, int udpPort, NodeId nodeId) {
        this.ipv4 = ipv4;
        this.ipv6 = ipv6;
        this.tcpPort = tcpPort;
        this.udpPort = udpPort;
        this.nodeId = nodeId;
    }
}
