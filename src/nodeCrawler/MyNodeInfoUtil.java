package nodeCrawler;

import lombok.Getter;
import lombok.Setter;
import myDiscover.table.NodeId;

public class MyNodeInfoUtil {
    public String ipv4;//not null
    @Setter
    @Getter
    public String ipv6;//can be null
    //beneath 3 not null
    @Setter
    @Getter
    public int tcpPort;
    @Setter
    @Getter
    public int udpPort;
    public NodeId nodeId;
    //beneath can be null
    @Setter
    @Getter
    public String walletName;
    @Setter
    @Getter
    public int activeConnNum;
    @Setter
    @Getter
    public int passiveConnNum;
    @Setter
    @Getter
    public String codeVersion;
    @Setter
    @Getter
    public int configActiveNodeSize;
    @Setter
    @Getter
    public int configMaxConnections;
    @Setter
    @Getter
    public int configPassiveNodeSize;
    @Setter
    @Getter
    public int sameIpMaxConnections;
    @Setter
    @Getter
    public int seedNodesSize;
    @Setter
    @Getter
    public boolean supportConstant;
    @Setter
    @Getter
    public String versionNum;
    @Setter
    @Getter
    public String javaVersion;
    @Setter
    @Getter
    public int cpuCount;
    @Setter
    @Getter
    public String osName;
    @Setter
    @Getter
    public byte[] online_days;
    @Setter
    @Getter
    public int online_time;


    public MyNodeInfoUtil(String ipv4, int udpPort, NodeId nodeId) {
        this.ipv4 = ipv4;
        this.udpPort = udpPort;
        this.nodeId = nodeId;
    }
    public MyNodeInfoUtil(String ipv4,NodeId nodeId,int tcpPort, int udpPort) {
        this.ipv4 = ipv4;
        this.udpPort = udpPort;
        this.nodeId = nodeId;
        this.tcpPort = tcpPort;
    }
    public MyNodeInfoUtil(String ipv4, String ipv6, int tcpPort,
                          int udpPort, NodeId nodeId, String walletName,
                          int activeConnNum, int passiveConnNum, String codeVersion,
                          int configActiveNodeSize, int configMaxConnections,
                          int configPassiveNodeSize, int sameIpMaxConnections,
                          int seedNodesSize, boolean supportConstant, String versionNum,
                          String javaVersion, int cpuCount, String osName, byte[] online_days, int online_time) {
        this.ipv4 = ipv4;
        this.ipv6 = ipv6;
        this.tcpPort = tcpPort;
        this.udpPort = udpPort;
        this.nodeId = nodeId;
        this.walletName = walletName;
        this.activeConnNum = activeConnNum;
        this.passiveConnNum = passiveConnNum;
        this.codeVersion = codeVersion;
        this.configActiveNodeSize = configActiveNodeSize;
        this.configMaxConnections = configMaxConnections;
        this.configPassiveNodeSize = configPassiveNodeSize;
        this.sameIpMaxConnections = sameIpMaxConnections;
        this.seedNodesSize = seedNodesSize;
        this.supportConstant = supportConstant;
        this.versionNum = versionNum;
        this.javaVersion = javaVersion;
        this.cpuCount = cpuCount;
        this.osName = osName;
        this.online_days = online_days;
        this.online_time = online_time;
    }


}
