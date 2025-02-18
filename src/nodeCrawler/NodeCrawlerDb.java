package nodeCrawler;

import myDiscover.Tool;
import myDiscover.message.MyNeighborsMessage;
import myDiscover.table.NodeId;
import org.tron.p2p.discover.Node;

import java.sql.*;

public class NodeCrawlerDb {
    public String url = "jdbc:mysql://localhost:3306/node";
    public String user = "root";
    public String password = "root";
    public String database = "node";
    public String tableName = "nodes";
    public int dbOldSize = 0;
    public int dbCurrentSize = 0;
    public String sizeQuery = "select count(*) from " + tableName;
    public String singleDataQueryById = "select * from " + tableName + " where id = ";
    public String batchInsert = "INSERT INTO "+tableName+" (id, column1, column2, column3) " +
            "VALUES (?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "column1 = VALUES(column1), " +
            "column2 = VALUES(column2), " +
            "column3 = VALUES(column3)";

    public NodeCrawlerDb() {
    }

    public Connection getConnection() throws SQLException {
        Connection conn;
        conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

    public void updateDbSize(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sizeQuery);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            dbOldSize = dbCurrentSize;
            dbCurrentSize = rs.getInt(1);
            System.out.println("table have " + dbCurrentSize + " rows for now");
        } else {
            System.out.println("get table size failed");
        }
    }

    public MyNodeInfoUtil queryNodeInfoById(int id, Connection conn) throws SQLException {
        PreparedStatement psIn = conn.prepareStatement(singleDataQueryById + id);
        ResultSet rsIn = psIn.executeQuery();
        if (rsIn.next()) {
            String dstIp = rsIn.getString("ipv4");
            int dstPort = rsIn.getInt("port");
            byte[] data = rsIn.getBytes("nodeId");
            return new MyNodeInfoUtil(dstIp, dstPort, new NodeId(Tool.toByteArray(data)));
        } else {
            return null;
        }
    }
    public int nodeInfoBatchInsert(MyNeighborsMessage msg, Connection conn) throws SQLException {
        for (Node node : msg.getNodes()) {
            //TODO:批量插入节点信息
        }
    }
}