package nodeCrawler.Test;

import myDiscover.Tool;
import nodeCrawler.NodeCrawlerDb;
import nodeCrawler.NodeTestConnection.MyNodeCrawlerConnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TronscanNodesAliveTest {
    private static int startNum=1;
    private static int endNum=1;
    public static void main(String[] args) {
        try {
            NodeCrawlerDb db = new NodeCrawlerDb();
            Connection conn = db.getConnection();
            PreparedStatement ps = conn.prepareStatement(db.tronscanNodesSizeQuery);
            ResultSet rs =ps.executeQuery();
            int tableSize= 0;
            if (rs.next()) {
                tableSize = rs.getInt(1);
                System.out.println("table size: "+tableSize);
            }
            if (tableSize == 0) {
                System.out.println("No tronscan nodes found");
                return;
            }
            ps.close();
            while (startNum<=tableSize){
                MyNodeCrawlerConnector connector = new MyNodeCrawlerConnector("10.21.213.106");
                connector.init();
                endNum+=db.queryTronscanNodesBatchSize;
                if(endNum>tableSize){
                    endNum=tableSize;
                }
                System.out.println("startNum: "+startNum+" endNum: "+endNum);
                PreparedStatement ps2 = conn.prepareStatement(db.tronscanNodesQuery+(startNum-1));
                ResultSet rs2 = ps2.executeQuery();
                System.out.println("got rs2");
                ArrayList<String> ipList  = new ArrayList<>();
                while (rs2.next()) {
                    ipList.add(rs2.getString(1));
                }
                rs2.close();
                ps2.close();
                ExecutorService executorService = Executors.newFixedThreadPool(5);
                int count = 0;
                for (String ip : ipList) {
                    count++;
                    int finalCount = count;
                    executorService.submit(()->{
                        System.out.println("task "+ finalCount +" submitted");
                        try{
                            processOneNode(ip, connector, db, conn);
                        } catch (SQLException e) {
                            System.out.println("sql error "+e.getMessage());
                            throw new RuntimeException(e);
                        } catch (InterruptedException | IOException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });

                }
                executorService.shutdown();
                while (!executorService.awaitTermination(2, TimeUnit.SECONDS)){
                    Thread.sleep(1_000);
                }
                System.out.println("one batch alive detection finished");
                startNum=endNum+1;
                //ps2.close();
                connector.close();
                System.out.println("starting next loop");
            }
            conn.close();
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void processOneNode(String ipv4, MyNodeCrawlerConnector connector, NodeCrawlerDb db, Connection conn) throws SQLException, InterruptedException, IOException {
        System.out.println("checking "+ipv4);
        byte[] nodeId = connector.nodeIsReachable(ipv4,18888, Tool.generateRandomNodeId(),Tool.getAvailablePort());
        if (nodeId!=null) {
            System.out.println("ip "+ipv4+" is online, updating database now");
            int affectedRows = db.toggleOnlineByIp(conn,ipv4, db.updateOneNodeInTronscanNodesTable1);
            System.out.println("update "+affectedRows+" rows");
        }
        else {
            System.out.println("ip "+ipv4+" is offline");
//            int affectedRows = db.toggleOnlineByIp(conn,ipv4, db.updateOneNodeInTronscanNodesTable2);
//            System.out.println("update "+affectedRows+" rows");
        }
    }
}
