package nodeCrawler;

import lombok.Getter;
import lombok.Setter;
import myDiscover.Tool;
import myDiscover.table.NodeId;

import java.sql.ResultSet;

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
    @Setter
    @Getter
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
    @Getter
    @Setter
    public long lastOnlineDetect;

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
                          String javaVersion, int cpuCount, String osName, byte[] online_days,
                          int online_time,long lastOnlineDetect) {
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
        this.lastOnlineDetect = lastOnlineDetect;
    }

    public MyNodeInfoUtil(MyNodeInfoUtil old, byte[] newNodeId){
        this.ipv4 = old.ipv4;
        this.ipv6 = old.ipv6;
        this.tcpPort = old.tcpPort;
        this.udpPort = old.udpPort;
        this.nodeId = new NodeId(Tool.toByteArray(newNodeId));
        this.walletName = old.walletName;
        this.activeConnNum = old.activeConnNum;
        this.passiveConnNum = old.passiveConnNum;
        this.codeVersion = old.codeVersion;
        this.configActiveNodeSize = old.configActiveNodeSize;
        this.configMaxConnections = old.configMaxConnections;
        this.configPassiveNodeSize = old.configPassiveNodeSize;
        this.sameIpMaxConnections = old.sameIpMaxConnections;
        this.seedNodesSize = old.seedNodesSize;
        this.supportConstant = old.supportConstant;
        this.versionNum = old.versionNum;
        this.javaVersion = old.javaVersion;
        this.cpuCount = old.cpuCount;
        this.osName = old.osName;
        this.online_days = old.online_days;
        this.online_time = old.online_time;
        this.lastOnlineDetect = old.lastOnlineDetect;
    }

    public static MyNodeInfoUtil getInstanceFromMysqlRes(ResultSet rsIn) {
        try {
            String dstIp = rsIn.getString("ipv4");
            String ipv6 = rsIn.getString("ipv6");
            String walletName = rsIn.getString("walletName");
            int activeConnNum = rsIn.getInt("activeConnNum");
            int passiveConnNum = rsIn.getInt("passiveConnNum");
            String codeVersion = rsIn.getString("codeVersion");
            int configActiveNodeSize = rsIn.getInt("configActiveNodeSize");
            int configMaxConnections = rsIn.getInt("configMaxConnections");
            int configPassiveNodeSize = rsIn.getInt("configPassiveNodeSize");
            int sameIpMaxConnections = rsIn.getInt("sameIpMaxConnections");
            int seedNodesSize = rsIn.getInt("seedNodesSize");
            boolean supportConstant = rsIn.getBoolean("supportConstant");
            String versionNum = rsIn.getString("versionNum");
            String javaVersion = rsIn.getString("javaVersion");
            int cpuCount = rsIn.getInt("cpuCount");
            String osName = rsIn.getString("osName");
            byte[] online_days = rsIn.getBytes("online_days");
            int online_time = rsIn.getInt("online_time");
            int dstPort = rsIn.getInt("udpPort");
            int tcpPort = rsIn.getInt("tcpPort");
            byte[] nodeId = rsIn.getBytes("nodeId");
            long lastOnlineDetect = rsIn.getTimestamp("lastOnlineDetect").getTime();
            return new MyNodeInfoUtil(dstIp,ipv6,tcpPort,dstPort,
                    new NodeId(Tool.toByteArray(nodeId)),walletName,
                    activeConnNum,passiveConnNum,codeVersion,
                    configActiveNodeSize,configMaxConnections,
                    configPassiveNodeSize,sameIpMaxConnections,
                    seedNodesSize,supportConstant,versionNum,
                    javaVersion,cpuCount,osName,online_days,
                    online_time,lastOnlineDetect);
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


}
