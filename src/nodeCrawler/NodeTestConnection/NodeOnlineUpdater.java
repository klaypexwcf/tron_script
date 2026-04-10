package nodeCrawler.NodeTestConnection;

import lombok.extern.slf4j.Slf4j;
import myDiscover.Tool;
import nodeCrawler.MyNodeInfoUtil;
import nodeCrawler.NodeCrawlerDb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j(topic = "nodeOnlineUpdater")
public class NodeOnlineUpdater {
    private final String localIp;
    public static final int MAX_TASKS = 5;
    public static final int QUERY_BATCH_SIZE = 25;
//    List<CompletableFuture<Void>> futures = new ArrayList<>();

    public NodeOnlineUpdater(String localIp) {
        this.localIp = localIp;
    }

    public void startOnlineUpdater(){
        try {
            NodeCrawlerDb db = new NodeCrawlerDb();
            Connection conn = db.getConnection();
            db.toggleTableStatus(conn,db.publicOnlineNodesTableName,
                    NodeCrawlerDb.TableStatus.OFFLINE.getValue(), NodeCrawlerDb.TableStatus.ONLINE.getValue());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(()->{
            try {
                updateAllNodes();
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        },0,2, TimeUnit.HOURS);
        log.info("online updater started, now {}",System.currentTimeMillis());
        System.out.println("online updater started, now "+System.currentTimeMillis());
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try {
                NodeCrawlerDb db = new NodeCrawlerDb();
                Connection conn = db.getConnection();
                db.toggleTableStatus(conn,db.publicOnlineNodesTableName,
                        NodeCrawlerDb.TableStatus.ONLINE.getValue(), NodeCrawlerDb.TableStatus.OFFLINE.getValue());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
    }
    private void updateAllNodes() throws SQLException, InterruptedException {
        int tasksSubmitted = 0;
        AtomicInteger onlineNewDetect = new AtomicInteger();
        LocalDateTime now = LocalDateTime.now();
        log.info("updating all nodes");
        NodeCrawlerDb db = new NodeCrawlerDb();
        System.out.println("db instance created");
        Connection conn = db.getConnection();
        System.out.println("got db connection");
        db.toggleTableStatus(conn,db.publicOnlineNodesTableName,
                NodeCrawlerDb.TableStatus.ONLINE.getValue(),NodeCrawlerDb.TableStatus.UPDATING.getValue());
        db.trimPublicOnlineTable(conn);
        log.info("table cleared");
        int maxLines = db.getTableSize(conn);
        log.info("got max lines: {}", maxLines);
        int startNum =1;
        int endNum =1;
        while (startNum <= maxLines){
            ExecutorService executor = Executors.newFixedThreadPool(MAX_TASKS);

            endNum+=QUERY_BATCH_SIZE;
            if (endNum > maxLines){
                endNum = maxLines;
            }
            System.out.println("startNum: "+startNum+" endNum: "+endNum);
            ArrayList<MyNodeInfoUtil> list = db.queryBatchNodeInfoByCreateTimeOrder(startNum,conn);
            int count = 0;
            for (MyNodeInfoUtil nodeInfo : list){
                count++;
                int t = count;
                long lastOnlineDetect = nodeInfo.getLastOnlineDetect();
                long currentTimeMillis = System.currentTimeMillis();
                //分布式在线探测下，粗略防止多次更新一个节点的在线情况
                if (Math.abs(lastOnlineDetect - currentTimeMillis) > Duration.ofHours(4).toMillis()) {
                    tasksSubmitted++;
                    executor.submit(()->{
                        System.out.println("task "+ t +" submitted");
                        try{
                            MyNodeCrawlerConnector connector = new MyNodeCrawlerConnector(localIp);
                            connector.init();
                            byte[] nodeId = connector.nodeIsReachable(nodeInfo.ipv4,
                                    nodeInfo.tcpPort, Tool.generateRandomNodeId(), Tool.getAvailablePort());
                            log.info("node {} {}",nodeInfo.ipv4,(nodeId!=null ? "online" : "offline"));
                            System.out.println("node "+nodeInfo.ipv4+" "+(nodeId!=null ? "online" : "offline"));
                            //节点在线，更新public nodes的在线时间和nodeId，然后将该记录插入public online nodes
                            if(nodeId!=null) {
                                onlineNewDetect.getAndIncrement();
                                MyNodeInfoUtil newNodeInfo = new MyNodeInfoUtil(nodeInfo,nodeId);
                                db.updateOneLineToOriTable(newNodeInfo, conn, newNodeInfo.ipv4, newNodeInfo.tcpPort,newNodeInfo.udpPort);
                                db.insertOrUpdateOneRowToPublicOnlineTable(newNodeInfo, conn, newNodeInfo.ipv4, newNodeInfo.tcpPort,newNodeInfo.udpPort);
                            }
                            else {
                                db.updateDetectTimeOnly(conn,nodeInfo.ipv4,nodeInfo.tcpPort,nodeInfo.udpPort);

                            }
                            connector.close();
                        } catch (InterruptedException | SQLException | IOException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
            executor.shutdown();

            while (!executor.awaitTermination(2, TimeUnit.SECONDS)){
                Thread.sleep(1_000);
            }
            System.out.println("one batch alive detection finished");
            startNum=endNum+1;
            System.out.println("starting next loop");
        }
        LocalDateTime end = LocalDateTime.now();
        log.info("updating all nodes finished, taking time: {}, {} tasks submitted, {} online nodes detected ", Duration.between(now,end).toMillis(),tasksSubmitted,onlineNewDetect.get());
        db.toggleTableStatus(conn,db.publicOnlineNodesTableName,
                NodeCrawlerDb.TableStatus.UPDATING.getValue(),NodeCrawlerDb.TableStatus.ONLINE.getValue());
        conn.close();
    }
}
