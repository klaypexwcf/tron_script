package nodeCrawler;

import myDiscover.table.NodeId;

public class MyNodeInfoUtil {
    public String ipv4;//not null
    public String ipv6;//can be null
    //beneath 3 not null
    public int tcpPort;
    public int udpPort;
    public NodeId nodeId;
    //beneath can be null
    public String walletName;
    public int activeConnNum;
    public int passiveConnNum;
    public String codeVersion;
    public int configActiveNodeSize;
    public int configMaxConnections;
    public int configPassiveNodeSize;
    public int sameIpMaxConnections;
    public int seedNodesSize;
    public boolean supportConstant;
    public String versionNum;
    public String javaVersion;
    public int cpuCount;
    public String osName;


    public MyNodeInfoUtil(String ipv4, int udpPort, NodeId nodeId) {
        this.ipv4 = ipv4;
        this.udpPort = udpPort;
        this.nodeId = nodeId;
    }

    public MyNodeInfoUtil(String ipv4, String ipv6, int tcpPort, int udpPort,
                          NodeId nodeId, String walletName, int activeConnNum, int passiveConnNum,
                          String codeVersion, int configActiveNodeSize, int configMaxConnections,
                          int configPassiveNodeSize, int sameIpMaxConnections, int seedNodesSize,
                          boolean supportConstant, String versionNum, String javaVersion, int cpuCount, String osName) {
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
    }
}
