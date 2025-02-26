package nodeCrawler.Test;

import nodeCrawler.NodeCrawlerDb;

import java.util.Arrays;

public class NodeCrawlerDbTest {
    public static void main(String[] args) {
        byte[] tmp = new byte[255];
        NodeCrawlerDb.updateOnlineIntervals(tmp);
        System.out.println(Arrays.toString(tmp));
    }
}
