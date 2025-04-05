package nodeCrawler.Test;

import nodeCrawler.MyMessageHandlerForNodeCrawler;
import nodeCrawler.NodeCrawler;

import java.sql.SQLException;

public class NeighborsTest {
    public static void main(String[] args) {
        NodeCrawler nc = new NodeCrawler();

        try {
            MyMessageHandlerForNodeCrawler myMessageHandler = nc.neighborsListeningWork();
            for (int i = 0; i < 20; i++) {
                nc.sendFindNodeWithRandomDstId("81.70.23.5",30304,myMessageHandler);
                System.out.println("sent find_node "+i);
                Thread.sleep(15000);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
