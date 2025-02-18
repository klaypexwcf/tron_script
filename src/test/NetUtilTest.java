package test;

import org.tron.p2p.utils.NetUtil;

public class NetUtilTest {
    public static void main(String[] args) throws InterruptedException {
        while (true){
            Thread.sleep(1000);
            testExternalIP();
        }


    }
    private static void testExternalIP() {
        try {
            String externalIp = NetUtil.getExternalIpV4();
            if (externalIp == null || !externalIp.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
                System.err.println("Invalid or null IPv4 address: " + externalIp);
            } else {
                System.out.println("External IPv4: " + externalIp);
            }
        } catch (Exception e) {
            System.err.println("Failed to get external IPv4 address: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
