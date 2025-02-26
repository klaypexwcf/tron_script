package nodeCrawler.NodeTestConnection;

import lombok.extern.slf4j.Slf4j;
import myDiscover.Tool;
import nodeCrawler.MyNodeInfoUtil;
import nodeCrawler.NodeCrawlerDb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j(topic = "nodeOnlineUpdater")
public class NodeOnlineUpdater {
    private final String localIp;
//    private final ExecutorService executor = Executors.newFixedThreadPool(10);
//    List<CompletableFuture<Void>> futures = new ArrayList<>();

    public NodeOnlineUpdater(String localIp) {
        this.localIp = localIp;
    }

    public void startOnlineUpdater(){
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(()->{
            try {
                updateAllNodes();
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        },0,4, TimeUnit.HOURS);
        log.info("online updater started");
    }
    private void updateAllNodes() throws SQLException, InterruptedException {
        log.info("updating all nodes");
        NodeCrawlerDb db = new NodeCrawlerDb();
        Connection conn = db.getConnection();
        db.clearTable(conn);
        int maxLines = db.getTableSize(conn);
        //conn.close();
        if(maxLines > 0){
            for(int i = 1; i <= maxLines; i++){
                log.info("updating rows "+i);
                try{
                    MyNodeCrawlerConnector connector = new MyNodeCrawlerConnector(localIp);
                    connector.init();
                    MyNodeInfoUtil nodeInfo = db.queryNodeInfoByCreateTimeOrder(i, conn);
                    //System.out.println("tcp port: "+nodeInfo.tcpPort);
                    boolean isOnline = connector.nodeIsReachable(nodeInfo.ipv4,
                            nodeInfo.tcpPort, Tool.generateRandomNodeId(), Tool.getAvailablePort());
                    log.info("node {} {}",nodeInfo.ipv4,isOnline ? "online" : "offline");
                    if(isOnline) {
                        db.insertOneLineToOnlineTable(nodeInfo, conn);
                        db.updateOneLineToOriTable(nodeInfo, conn);
                    }
                    connector.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("all tasks for update online nodes finished");
            conn.close();
            //connector.close();
            return;
        }
        else{
            System.out.println("get table size failed");
            conn.close();
            //connector.close();
            return;
        }

    }
}
