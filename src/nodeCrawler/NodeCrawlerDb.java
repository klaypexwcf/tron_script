package nodeCrawler;

import myDiscover.Tool;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.kad.NeighborsMessage;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class NodeCrawlerDb {
    private static final LocalDate START_DATE = LocalDate.of(2025, 1, 1);
    private static final int TOTAL_YEARS = 100; // 你可以根据存储大小调整

    private static int calculateTotalDays() {
        int days = 0;
        for (int i = 0; i < TOTAL_YEARS; i++) {
            int year = START_DATE.getYear() + i;
            days += LocalDate.of(year, 12, 31).getDayOfYear(); // 获取该年的总天数
        }
        return days;
    }

    public String database = "tron";
    public String url = "jdbc:mysql://81.70.23.5:3306/"+database;
    public String user = "tron";
    public String password = "Wcf314159";
    public String tableName = "tron_nodes";
    public String onlineNodesTableNodes = "online_nodes";
    public int dbOldCursorLineNUm = 0;
    public int dbCurrentCursorLineNum = 0;

    public static int MAX_SEND_BATCH_SIZE = 15;
    public String sizeQuery = "select count(*) from " + tableName;
    public String singleDataQueryByCreateTimeOrder = "select * from " + tableName + " ORDER BY create_time LIMIT 1 OFFSET ";
    public String queryByPriKey = "select * from " + tableName + " WHERE ipv4 = ? AND nodeID = ? ";
    public String clearTable = "TRUNCATE TABLE "+onlineNodesTableNodes;
    public String insertForTable1 = "INSERT INTO " + tableName + " (ipv4, ipv6, tcpPort, udpPort, nodeId, " +
            "walletName, activeConnNum, passiveConnNum, codeVersion, configActiveNodeSize, configMaxConnections, " +
            "configPassiveNodeSize, sameIpMaxConnections, seedNodesSize, supportConstant, versionNum, " +
            "javaVersion, cpuCount, osName, online_days, online_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "ipv6 = VALUES(ipv6), tcpPort = VALUES(tcpPort), udpPort = VALUES(udpPort), " +
            "walletName = VALUES(walletName), activeConnNum = VALUES(activeConnNum), " +
            "passiveConnNum = VALUES(passiveConnNum), codeVersion = VALUES(codeVersion), " +
            "configActiveNodeSize = VALUES(configActiveNodeSize), configMaxConnections = VALUES(configMaxConnections), " +
            "configPassiveNodeSize = VALUES(configPassiveNodeSize), sameIpMaxConnections = VALUES(sameIpMaxConnections), " +
            "seedNodesSize = VALUES(seedNodesSize), supportConstant = VALUES(supportConstant), versionNum = VALUES(versionNum), " +
            "javaVersion = VALUES(javaVersion), cpuCount = VALUES(cpuCount), osName = VALUES(osName), online_days = VALUES(online_days), " +
            "online_time = VALUES(online_time);";

    public String insertForTable2 = "INSERT INTO " + onlineNodesTableNodes + " (ipv4, ipv6, tcpPort, udpPort, nodeId, " +
            "walletName, activeConnNum, passiveConnNum, codeVersion, configActiveNodeSize, configMaxConnections, " +
            "configPassiveNodeSize, sameIpMaxConnections, seedNodesSize, supportConstant, versionNum, " +
            "javaVersion, cpuCount, osName, online_days, online_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "ipv6 = VALUES(ipv6), tcpPort = VALUES(tcpPort), udpPort = VALUES(udpPort), " +
            "walletName = VALUES(walletName), activeConnNum = VALUES(activeConnNum), " +
            "passiveConnNum = VALUES(passiveConnNum), codeVersion = VALUES(codeVersion), " +
            "configActiveNodeSize = VALUES(configActiveNodeSize), configMaxConnections = VALUES(configMaxConnections), " +
            "configPassiveNodeSize = VALUES(configPassiveNodeSize), sameIpMaxConnections = VALUES(sameIpMaxConnections), " +
            "seedNodesSize = VALUES(seedNodesSize), supportConstant = VALUES(supportConstant), versionNum = VALUES(versionNum), " +
            "javaVersion = VALUES(javaVersion), cpuCount = VALUES(cpuCount), osName = VALUES(osName), online_days = VALUES(online_days), " +
            "online_time = VALUES(online_time);";

    public NodeCrawlerDb() {
    }

    public Connection getConnection() throws SQLException {
        Connection conn;
        conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
    public void clearTable(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(clearTable);
        ps.executeUpdate();
        ps.close();
    }

    public void resetCursorLineNum(){
        dbOldCursorLineNUm = 0;
        dbCurrentCursorLineNum = 0;
    }
    public int getTableSize(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sizeQuery);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            int tmp = rs.getInt(1);
            rs.close();
            return tmp;
        }
        else{
            System.out.println("get table size failed");
            rs.close();
            return 0;
        }
    }
    public void updateTableSize(Connection conn) throws SQLException {
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
    public void insertOneLineToOnlineTable(MyNodeInfoUtil nodeInfo,Connection conn) throws SQLException {
        updateOrInsertOneLineInTable(nodeInfo, conn, insertForTable2);
    }
    public void updateOneLineToOriTable(MyNodeInfoUtil nodeInfo,Connection conn) throws SQLException {
        updateOrInsertOneLineInTable(nodeInfo, conn, insertForTable1);
    }

    private void updateOrInsertOneLineInTable(MyNodeInfoUtil nodeInfo, Connection conn, String mysqlStatement) throws SQLException {
        //System.out.println("entering method  updateOrInsertOneLineInTable");
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement(mysqlStatement);
        stmt.setString(1, nodeInfo.ipv4);
        stmt.setString(2,nodeInfo.ipv6);
        stmt.setInt(3,nodeInfo.tcpPort);
        stmt.setInt(4, nodeInfo.udpPort);
        stmt.setBytes(5, nodeInfo.nodeId.getLowerBytesId());
        setRpcInfo(nodeInfo, stmt);
        byte[] tmp =new byte[255];
        if (nodeInfo.online_days!=null){
            tmp = nodeInfo.online_days;
        }
        //System.out.println("setRpcInfo finished");
        updateOnlineIntervals(tmp);
        //System.out.println("getting updated online_days succeed");
        stmt.setBytes(20,tmp);
        stmt.setInt(21,nodeInfo.getOnline_time()+1);
        //System.out.println("ready to insert");
        stmt.executeUpdate();
        int affectedRows = stmt.executeUpdate();
        //System.out.println("rows affected: " + affectedRows);
        stmt.close();
    }

    private void setRpcInfo(MyNodeInfoUtil nodeInfo, PreparedStatement stmt) throws SQLException {
        stmt.setString(6,nodeInfo.getWalletName());
        stmt.setInt(7,nodeInfo.getActiveConnNum());
        stmt.setInt(8,nodeInfo.getPassiveConnNum());
        stmt.setString(9,nodeInfo.getCodeVersion());
        stmt.setInt(10,nodeInfo.getConfigActiveNodeSize());
        stmt.setInt(11,nodeInfo.getConfigMaxConnections());
        stmt.setInt(12,nodeInfo.getConfigPassiveNodeSize());
        stmt.setInt(13,nodeInfo.getSameIpMaxConnections());
        stmt.setInt(14,nodeInfo.getSeedNodesSize());
        stmt.setBoolean(15, nodeInfo.isSupportConstant());
        stmt.setString(16, nodeInfo.getVersionNum());
        stmt.setString(17, nodeInfo.getJavaVersion());
        stmt.setInt(18, nodeInfo.getCpuCount());
        stmt.setString(19, nodeInfo.getOsName());
    }

    public static void updateOnlineIntervals(byte[] onlineIntervals) {
        LocalDateTime now = LocalDateTime.now();
        long intervalIndex = ChronoUnit.HOURS.between(START_DATE.atStartOfDay(), now) / 4; // 计算当前是第几个4小时段

//        if (intervalIndex < 0 || intervalIndex >= TOTAL_INTERVALS) {
//            throw new IllegalArgumentException("时间超出范围");
//        }

        int byteIndex = (int) (intervalIndex / 8); // 计算在哪个字节
        int bitIndex = (int) (intervalIndex % 8);  // 计算字节内的位索引

        onlineIntervals[byteIndex] |= (1 << (7 - bitIndex)); // 设置对应位为1（从高位到低位）
    }

    public MyNodeInfoUtil queryNodeInfoByCreateTimeOrder(int lineNum, Connection conn) throws SQLException {
        PreparedStatement psIn = conn.prepareStatement(singleDataQueryByCreateTimeOrder + (lineNum-1));
        ResultSet rsIn = psIn.executeQuery();
        if (rsIn.next()) {
            String dstIp = rsIn.getString("ipv4");
            int dstPort = rsIn.getInt("udpPort");
            int tcpPort = rsIn.getInt("tcpPort");
            byte[] data = rsIn.getBytes("nodeId");
            psIn.close();
            return new MyNodeInfoUtil(dstIp, new NodeId(Tool.toByteArray(data)),tcpPort, dstPort);
        } else {
            psIn.close();
            return null;
        }
    }
    public int nodeInfoBatchInsert(NeighborsMessage msg, Connection conn) throws SQLException {
        int count =0;
        PreparedStatement stmt = conn.prepareStatement(insertForTable1);
        for (Node node : msg.getNodes()) {

            String ipv4 = node.getInetSocketAddressV4().getHostString();
            stmt.setString(1, ipv4);
            byte[] nodeId = node.getId();
            if(node.getInetSocketAddressV6()==null){
                stmt.setNull(2,Types.VARCHAR);
            }
            else{
                stmt.setString(2,node.getInetSocketAddressV6().getHostString());
            }
            stmt.setInt(3,node.getPort());
            stmt.setInt(4,node.getPort());
            stmt.setBytes(5,node.getId());
            MyNodeInfoUtil nodeInfo = queryByPriKey(conn,ipv4,nodeId);
            if (nodeInfo != null) {
                setRpcInfo(nodeInfo, stmt);
                stmt.setBytes(20,nodeInfo.getOnline_days());
                stmt.setInt(21,nodeInfo.getOnline_time());

            }
            else {
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
                byte[] online_days = new byte[255];
                stmt.setBytes(20,online_days);
                stmt.setInt(21,0);
            }

            stmt.addBatch();
            count++;
        }
        stmt.executeBatch();
        stmt.close();
        return count;
    }
    public MyNodeInfoUtil queryByPriKey(Connection conn, String ipv4,byte[] nodeId) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(queryByPriKey);
        stmt.setString(1,ipv4);
        stmt.setBytes(2,nodeId);
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
            NodeId nodeIdWrap = new NodeId(Tool.toByteArray(nodeId));
            MyNodeInfoUtil node = new MyNodeInfoUtil(ipv4,rs.getInt("udpPort"),nodeIdWrap);
            node.setIpv6(rs.getString("ipv6"));
            node.setTcpPort(rs.getInt("tcpPort"));
            node.setUdpPort(rs.getInt("udpPort"));
            node.setWalletName(rs.getString("walletName"));
            node.setActiveConnNum(rs.getInt("activeConnNum"));
            node.setPassiveConnNum(rs.getInt("passiveConnNum"));
            node.setCodeVersion(rs.getString("codeVersion"));
            node.setConfigActiveNodeSize(rs.getInt("configActiveNodeSize"));
            node.setConfigPassiveNodeSize(rs.getInt("configPassiveNodeSize"));
            node.setConfigMaxConnections(rs.getInt("configMaxConnections"));
            node.setSameIpMaxConnections(rs.getInt("sameIpMaxConnections"));
            node.setSeedNodesSize(rs.getInt("seedNodesSize"));
            node.setSupportConstant(rs.getBoolean("supportConstant"));
            node.setVersionNum(rs.getString("versionNum"));
            node.setJavaVersion(rs.getString("javaVersion"));
            node.setCpuCount(rs.getInt("cpuCount"));
            node.setOsName(rs.getString("osName"));
            node.setOnline_days(rs.getBytes("online_days"));
            node.setOnline_time(rs.getInt("online_time"));

            return node;
        }
        else{
            return null;
        }
    }
}