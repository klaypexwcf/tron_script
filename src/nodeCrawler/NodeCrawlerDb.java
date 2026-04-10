package nodeCrawler;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import myDiscover.Tool;
import myDiscover.table.NodeId;
import nodeCrawler.NodeTestConnection.NodeOnlineUpdater;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.kad.NeighborsMessage;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;

@Slf4j
public class NodeCrawlerDb {
    private static final LocalDate START_DATE = LocalDate.of(2026, 1, 1);


    //在这里设置数据库信息
    public static final int LENGTH_OF_MYSQL_BLOB = 16384;
    public String database = "tron";
    public String url = "jdbc:mysql://localhost:3306/"+database+"?connectTimeout=5000";
    public String user = "root";
    public String password = "Wcf314159";
    public String publicNodesTableName = "public_nodes_test";
    public String publicOnlineNodesTableName = "public_online_nodes_test";
    public String tronscanNodesTableName = "tronscan_nodes";
    //在这里设置数据库信息


    public int queryTronscanNodesBatchSize = 40;
    public int dbOldCursorLineNUm = 0;
    public int dbCurrentCursorLineNum = 0;

    public static int MAX_SEND_BATCH_SIZE = 15;
    public String commonSizeQuery = "select count(*) from " + publicNodesTableName;
    public String singlePublicNodesQueryByCreateTimeOrder = "select * from " + publicNodesTableName + " ORDER BY create_time LIMIT 1 OFFSET ";
    public String batchPublicNodesQueryByCreateTimeOrder = "select * from " + publicNodesTableName + " ORDER BY create_time LIMIT "+ NodeOnlineUpdater.QUERY_BATCH_SIZE +" OFFSET ";
    public String queryByPriKey = "select * from " + publicNodesTableName + " WHERE ipv4 = ? AND udpPort = ? AND tcpPort = ?";
    public String trimPublicOnlineTable = "DELETE FROM "+publicOnlineNodesTableName +" WHERE lastOnlineDetect < NOW() - INTERVAL 4 HOUR;";
    public String updateLastOnlineDetectInPublicNodes = "update "+publicNodesTableName+" set lastOnlineDetect = ? where ipv4 = ? and tcpPort = ? and udpPort = ?";
    public String insertPublicNodesTable = "INSERT INTO " + publicNodesTableName + " (ipv4, ipv6, tcpPort, udpPort, nodeId, " +
            "walletName, activeConnNum, passiveConnNum, codeVersion, configActiveNodeSize, configMaxConnections, " +
            "configPassiveNodeSize, sameIpMaxConnections, seedNodesSize, supportConstant, versionNum, " +
            "javaVersion, cpuCount, osName, online_days, online_time, lastOnlineDetect) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "ipv6 = VALUES(ipv6), nodeId = VALUES(nodeId), " +
            "walletName = VALUES(walletName), activeConnNum = VALUES(activeConnNum), " +
            "passiveConnNum = VALUES(passiveConnNum), codeVersion = VALUES(codeVersion), " +
            "configActiveNodeSize = VALUES(configActiveNodeSize), configMaxConnections = VALUES(configMaxConnections), " +
            "configPassiveNodeSize = VALUES(configPassiveNodeSize), sameIpMaxConnections = VALUES(sameIpMaxConnections), " +
            "seedNodesSize = VALUES(seedNodesSize), supportConstant = VALUES(supportConstant), versionNum = VALUES(versionNum), " +
            "javaVersion = VALUES(javaVersion), cpuCount = VALUES(cpuCount), osName = VALUES(osName), online_days = VALUES(online_days), " +
            "online_time = VALUES(online_time),lastOnlineDetect = VALUES(lastOnlineDetect);";

    public String insertPublicOnlineNodesTable = "INSERT INTO " + publicOnlineNodesTableName + " (ipv4, ipv6, tcpPort, udpPort, nodeId, " +
            "walletName, activeConnNum, passiveConnNum, codeVersion, configActiveNodeSize, configMaxConnections, " +
            "configPassiveNodeSize, sameIpMaxConnections, seedNodesSize, supportConstant, versionNum, " +
            "javaVersion, cpuCount, osName, online_days, online_time, lastOnlineDetect) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "ipv6 = VALUES(ipv6), nodeId = VALUES(nodeId), " +
            "walletName = VALUES(walletName), activeConnNum = VALUES(activeConnNum), " +
            "passiveConnNum = VALUES(passiveConnNum), codeVersion = VALUES(codeVersion), " +
            "configActiveNodeSize = VALUES(configActiveNodeSize), configMaxConnections = VALUES(configMaxConnections), " +
            "configPassiveNodeSize = VALUES(configPassiveNodeSize), sameIpMaxConnections = VALUES(sameIpMaxConnections), " +
            "seedNodesSize = VALUES(seedNodesSize), supportConstant = VALUES(supportConstant), versionNum = VALUES(versionNum), " +
            "javaVersion = VALUES(javaVersion), cpuCount = VALUES(cpuCount), osName = VALUES(osName), online_days = VALUES(online_days), " +
            "online_time = VALUES(online_time), lastOnlineDetect = VALUES(lastOnlineDetect);";

    public String tronscanNodesQuery = "select * from "+ tronscanNodesTableName +" ORDER BY create_time LIMIT "+queryTronscanNodesBatchSize+" OFFSET ";
    public String tronscanNodesSizeQuery = "select count(*) from "+ tronscanNodesTableName;
    public String updateOneNodeInTronscanNodesTable1 = "UPDATE " + tronscanNodesTableName +" SET is_online = 1 WHERE ipv4 = ?";
    public String updateOneNodeInTronscanNodesTable2 = "UPDATE " + tronscanNodesTableName +" SET is_online = 0 WHERE ipv4 = ?";
    public String toggleTableStatus = "UPDATE table_status SET status = ? WHERE table_name = ? AND status = ?";
    public enum TableStatus {
        ONLINE("online"),
        OFFLINE("offline"),
        UPDATING("updating");
        @Getter
        private final String value;
        private TableStatus(String value) {
            this.value = value;
        }
    }
    public NodeCrawlerDb() {
    }

    public Connection getConnection() {
        Connection conn;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println("failed to get connection "+e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }
    public boolean toggleTableStatus(Connection conn,String tableName,String oldStatus,String newStatus) throws SQLException{
        PreparedStatement ps=conn.prepareStatement(toggleTableStatus);
        ps.setString(1, newStatus);
        ps.setString(2, tableName);
        ps.setString(3, oldStatus);
        int count = ps.executeUpdate();
        if (count == 1) {
            log.info("toggle {} status from {} to {} succeeded",tableName,oldStatus,newStatus);
            return true;
        }
        else {
            log.info("toggle {} status from {} to {} failed",tableName,oldStatus,newStatus);
            return false;
        }

    }
    public int toggleOnlineByIp(Connection conn, String ip, String sql) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, ip);
        int rs = ps.executeUpdate();
        ps.close();
        return rs;
    }
    public void trimPublicOnlineTable(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(trimPublicOnlineTable);
        int count=  ps.executeUpdate();
        log.info("public online table trimmed, affected {} rows", count);
        ps.close();
    }

    public void resetCursorLineNum(){
        dbOldCursorLineNUm = 0;
        dbCurrentCursorLineNum = 0;
    }
    public int getTableSize(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(commonSizeQuery);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            int tmp = rs.getInt(1);
            rs.close();
            ps.close();
            return tmp;
        }
        else{
            System.out.println("get table size failed");
            rs.close();
            ps.close();
            return 0;
        }
    }
    public void updateTableSize(Connection conn) throws SQLException {
        PreparedStatement ps;
        ps = conn.prepareStatement(commonSizeQuery);
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
    public void insertOrUpdateOneRowToPublicOnlineTable(MyNodeInfoUtil newNodeInfo, Connection conn,String ipv4,int tcpPort,int udpPort) throws SQLException {
        updateOrInsertOneRowInTable(newNodeInfo, conn, insertPublicOnlineNodesTable,ipv4,tcpPort,udpPort);
        System.out.println("insert one row into public online nodes table");
    }
    public void updateOneLineToOriTable(MyNodeInfoUtil newNodeInfo,Connection conn,String ipv4,int tcpPort,int udpPort) throws SQLException {
        updateOrInsertOneRowInTable(newNodeInfo, conn, insertPublicNodesTable,ipv4,tcpPort,udpPort);
        System.out.println("update one row in public nodes table");
    }

    private void updateOrInsertOneRowInTable(MyNodeInfoUtil newNodeInfo, Connection conn, String mysqlStatement, String ipv4, int tcpPort, int udpPort) throws SQLException {
        //System.out.println("entering method updateOrInsertOneRowInTable");
        conn.setAutoCommit(true);
        PreparedStatement stmt = conn.prepareStatement(mysqlStatement);
        stmt.setString(1, ipv4);
        stmt.setString(2,newNodeInfo.ipv6);
        stmt.setInt(3,tcpPort);
        stmt.setInt(4, udpPort);
        stmt.setBytes(5, newNodeInfo.getNodeId().getLowerBytesId());
        setRpcInfo(newNodeInfo, stmt);
        byte[] tmp = Arrays.copyOf(newNodeInfo.online_days,newNodeInfo.online_days.length);
        System.out.println("tmp for node :"+newNodeInfo.ipv4+" :"+ Arrays.toString(tmp));
        //System.out.println("setRpcInfo finished");
        boolean isEdit =updateOnlineIntervals(tmp);
        //System.out.println("getting updated online_days succeed");
        stmt.setBytes(20,tmp);
        if (isEdit) {
            System.out.println("----------------------");
            System.out.println("add 1 to online_time "+newNodeInfo.ipv4+", now "+(newNodeInfo.getOnline_time()+1)+", old "+newNodeInfo.getOnline_time());
            stmt.setInt(21,newNodeInfo.getOnline_time()+1);
        }
        else {
            System.out.println("///////////////////");
            System.out.println("online_time is already set online");
            stmt.setInt(21,newNodeInfo.getOnline_time());
        }
        //更新探测时间戳
        stmt.setTimestamp(22,new Timestamp(System.currentTimeMillis()));
        //System.out.println("ready to insert");
        stmt.executeUpdate();
        //int affectedRows = stmt.executeUpdate();
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

    /**
     * 检查对应时间段值，将其设为1
     * @param onlineIntervals
     */
    public static boolean updateOnlineIntervals(byte[] onlineIntervals) {
        LocalDateTime now = LocalDateTime.now();
        long intervalIndex = ChronoUnit.HOURS.between(START_DATE.atStartOfDay(), now) / 4; // 计算当前是第几个4小时段

        int byteIndex = (int) (intervalIndex / 8); // 计算在哪个字节
        int bitIndex = (int) (intervalIndex % 8);  // 计算字节内的位索引

        byte mask = (byte) (1 << (7 - bitIndex)); // 计算要设置的位的掩码

        boolean wasZero = (onlineIntervals[byteIndex] & mask) == 0; // 检查该位是否原来是0

        onlineIntervals[byteIndex] |= mask; // 设置该位为1

        return wasZero; // 如果原来是0（即被设置为1），返回true，否则返回false
    }

    public MyNodeInfoUtil queryNodeInfoByCreateTimeOrder(int lineNum, Connection conn) throws SQLException {
        PreparedStatement psIn = conn.prepareStatement(singlePublicNodesQueryByCreateTimeOrder + (lineNum-1));
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
    public ArrayList<MyNodeInfoUtil> queryBatchNodeInfoByCreateTimeOrder(int startNum, Connection conn) throws SQLException {
        PreparedStatement psIn = conn.prepareStatement(batchPublicNodesQueryByCreateTimeOrder + (startNum-1));
        ResultSet rsIn = psIn.executeQuery();
        ArrayList<MyNodeInfoUtil> list = new ArrayList<>();
        while(rsIn.next()){
            list.add(MyNodeInfoUtil.getInstanceFromMysqlRes(rsIn));
        }
        //System.out.println("list: " + list);
        psIn.close();
        return list;
    }
    public int nodeInfoBatchInsert(NeighborsMessage msg, Connection conn) throws SQLException {
        log.info("processing neighbors msg received from {}",msg.getFrom().getHostV4());
        int count =0;
        PreparedStatement stmt = conn.prepareStatement(insertPublicNodesTable);
        for (Node node : msg.getNodes()) {

            String ipv4 = node.getInetSocketAddressV4().getHostString();
            stmt.setString(1, ipv4);
            //byte[] nodeId = node.getId();
            if(node.getInetSocketAddressV6()==null){
                stmt.setNull(2,Types.VARCHAR);
            }
            else{
                stmt.setString(2,node.getInetSocketAddressV6().getHostString());
            }
            stmt.setInt(3,node.getPort());
            stmt.setInt(4,node.getPort());
            stmt.setBytes(5,node.getId());
            MyNodeInfoUtil nodeInfo = queryByPriKey(conn,ipv4,node.getPort());
            //已有的记录，只需更新NodeId
            if (nodeInfo != null) {
                setRpcInfo(nodeInfo, stmt);
                stmt.setBytes(20,nodeInfo.getOnline_days());
                stmt.setInt(21,nodeInfo.getOnline_time());
                stmt.setTimestamp(22,new Timestamp(nodeInfo.getLastOnlineDetect()));
            }
            //初始化第一条记录
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
                stmt.setTimestamp(22,new Timestamp(1_000));
            }

            stmt.addBatch();
            count++;
        }
        stmt.executeBatch();
        stmt.close();
        return count;
    }
    public MyNodeInfoUtil queryByPriKey(Connection conn, String ipv4,int udpPort) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(queryByPriKey);
        stmt.setString(1, ipv4);
        stmt.setInt(2, udpPort);
        stmt.setInt(3, udpPort);
        ResultSet rs = stmt.executeQuery();

        try {
            if (rs.next()) {
                return MyNodeInfoUtil.getInstanceFromMysqlRes(rs);
            } else {
                return null;
            }
        } finally {
            // 确保先关闭 ResultSet，再关闭 PreparedStatement
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * 更新public nodes的在线探测时间lastOnlineDetect
     * @param conn
     * @param ipv4
     * @param tcpPort
     * @param udpPort
     */
    public void updateDetectTimeOnly(Connection conn, String ipv4, int tcpPort, int udpPort) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(updateLastOnlineDetectInPublicNodes);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        stmt.setTimestamp(1,now);
        stmt.setString(2,ipv4);
        stmt.setInt(3,tcpPort);
        stmt.setInt(4,udpPort);
        int updateCount = stmt.executeUpdate();
        if (updateCount != 1) {
            log.error("unexpected affect rows: {}, while updating ipv4: {}, tcpPort: {}, udpPort: {}", updateCount, ipv4, tcpPort, udpPort);
        }
        stmt.close();
    }

}