package nodeCrawler;

import myDiscover.Tool;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.kad.NeighborsMessage;

import java.sql.*;

public class NodeCrawlerDb {
    public String database = "tron";
    public String url = "jdbc:mysql://localhost:3306/"+database;
    public String user = "tron";
    public String password = "Wcf314159";
    public String tableName = "tron_nodes";
    public int dbOldCursorLineNUm = 0;
    public int dbCurrentCursorLineNum = 0;

    public static int MAX_SEND_BATCH_SIZE = 15;
    public String sizeQuery = "select count(*) from " + tableName;
    public String singleDataQueryByLine = "select * from " + tableName + " ORDER BY id LIMIT 1 OFFSET ";
    public String batchInsert = "INSERT INTO " + tableName + " (ipv4, ipv6, tcpPort, udpPort, nodeId, " +
            "walletName, activeConnNum, passiveConnNum, codeVersion, configActiveNodeSize, configMaxConnections, " +
            "configPassiveNodeSize, sameIpMaxConnections, seedNodesSize, supportConstant, versionNum, " +
            "javaVersion, cpuCount, osName) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "ipv4 = VALUES(ipv4), ipv6 = VALUES(ipv6), tcpPort = VALUES(tcpPort), udpPort = VALUES(udpPort), " +
            "nodeId = VALUES(nodeId), walletName = VALUES(walletName), activeConnNum = VALUES(activeConnNum), " +
            "passiveConnNum = VALUES(passiveConnNum), codeVersion = VALUES(codeVersion), " +
            "configActiveNodeSize = VALUES(configActiveNodeSize), configMaxConnections = VALUES(configMaxConnections), " +
            "configPassiveNodeSize = VALUES(configPassiveNodeSize), sameIpMaxConnections = VALUES(sameIpMaxConnections), " +
            "seedNodesSize = VALUES(seedNodesSize), supportConstant = VALUES(supportConstant), versionNum = VALUES(versionNum), " +
            "javaVersion = VALUES(javaVersion), cpuCount = VALUES(cpuCount), osName = VALUES(osName);";

    public NodeCrawlerDb() {
    }

    public Connection getConnection() throws SQLException {
        Connection conn;
        conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

    public void resetCursorLineNum(){
        dbOldCursorLineNUm = 0;
        dbCurrentCursorLineNum = 0;
    }
    public void updateDbSize(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sizeQuery);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            dbOldCursorLineNUm = dbCurrentCursorLineNum;
            dbCurrentCursorLineNum = rs.getInt(1);
            int fullSize= dbCurrentCursorLineNum;
            if(dbCurrentCursorLineNum - dbOldCursorLineNUm >MAX_SEND_BATCH_SIZE) {
                dbCurrentCursorLineNum = dbOldCursorLineNUm +MAX_SEND_BATCH_SIZE;
            }
            System.out.println("table have " + fullSize + " rows for now, set dbCurrentCursorLineNum to "+dbCurrentCursorLineNum);
        } else {
            System.out.println("get table size failed");
        }
        ps.close();
    }

    public MyNodeInfoUtil queryNodeInfoByLineNum(int lineNum, Connection conn) throws SQLException {
        PreparedStatement psIn = conn.prepareStatement(singleDataQueryByLine + (lineNum-1));
        ResultSet rsIn = psIn.executeQuery();
        if (rsIn.next()) {
            String dstIp = rsIn.getString("ipv4");
            int dstPort = rsIn.getInt("udpPort");
            byte[] data = rsIn.getBytes("nodeId");
            psIn.close();
            return new MyNodeInfoUtil(dstIp, dstPort, new NodeId(Tool.toByteArray(data)));
        } else {
            psIn.close();
            return null;
        }
    }
    public int nodeInfoBatchInsert(NeighborsMessage msg, Connection conn) throws SQLException {
        int count =0;
        PreparedStatement stmt = conn.prepareStatement(batchInsert);
        for (Node node : msg.getNodes()) {
            stmt.setString(1,node.getInetSocketAddressV4().getHostString());
            if(node.getInetSocketAddressV6()==null){
                stmt.setNull(2,Types.VARCHAR);
            }
            else{
                stmt.setString(2,node.getInetSocketAddressV6().getHostString());
            }
            stmt.setInt(3,node.getPort());
            stmt.setInt(4,node.getPort());
            stmt.setBytes(5,node.getId());
            stmt.setNull(6,java.sql.Types.VARCHAR);
            stmt.setNull(7,java.sql.Types.INTEGER);
            stmt.setNull(8,java.sql.Types.INTEGER);
            stmt.setNull(9,java.sql.Types.VARCHAR);
            stmt.setNull(10,java.sql.Types.INTEGER);
            stmt.setNull(11,java.sql.Types.INTEGER);
            stmt.setNull(12,java.sql.Types.INTEGER);
            stmt.setNull(13,java.sql.Types.INTEGER);
            stmt.setNull(14,java.sql.Types.INTEGER);
            stmt.setNull(15, java.sql.Types.TINYINT);
            stmt.setNull(16, java.sql.Types.VARCHAR);
            stmt.setNull(17, java.sql.Types.VARCHAR);
            stmt.setNull(18, java.sql.Types.INTEGER);
            stmt.setNull(19, java.sql.Types.VARCHAR);
            stmt.addBatch();
            count++;
        }
        stmt.executeBatch();
        stmt.close();
        return count;
    }
}