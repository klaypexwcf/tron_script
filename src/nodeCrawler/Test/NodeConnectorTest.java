package nodeCrawler.Test;

import myDiscover.Tool;
import nodeCrawler.NodeTestConnection.MyNodeCrawlerConnector;

public class NodeConnectorTest {
    public static void main(String[] args) throws InterruptedException {
        MyNodeCrawlerConnector nodeConnector = new MyNodeCrawlerConnector("10.2.20.9");
        nodeConnector.init();
//        for (int i = 0; i < 2; i++) {
//            boolean tmp1 = nodeConnector.nodeIsReachable("35.126.107.76"
//                    ,18888, Tool.generateRandomNodeId(),14586);
//            System.out.println(tmp1);
//        }
        boolean tmp = nodeConnector.nodeIsReachable("221.229.177.92"
                ,18888, Tool.generateRandomNodeId(),14585);
        System.out.println(tmp);
        nodeConnector.close();
        System.out.println("test stopped");
        System.out.println();
    }
}
