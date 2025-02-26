package nodeCrawler.NodeTestConnection;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.Getter;
import myConnection.socket.MyPeerClient;
import myDiscover.Tool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static nodeCrawler.NodeTestConnection.MyP2pChannelInitializerForNodeTestConnection.IS_SUCCESS;
public class MyNodeCrawlerConnector {
    private MyPeerClient myPeerClient;
    //本地IP
    @Getter
    private String localIp;

    public MyNodeCrawlerConnector(String localIp) {
        this.localIp = localIp;
    }


    public boolean init(){
        //MyConfig.init();
        myPeerClient = new MyPeerClient();
        myPeerClient.init();
        return true;
    }
    public void close(){
        System.out.println("closing connector");
        myPeerClient.close();
    }
    public boolean nodeIsReachable(String remoteIp, int remotePort, byte[] localId, int localPort) throws InterruptedException {
        //System.out.println("enter nodeIsReachable with parameters: "+remoteIp+":"+remotePort+":"+localPort);
        ChannelFuture channelFuture = myPeerClient
                .testConnection(Tool.wrapNode(remoteIp, remotePort, localId), localIp, localPort, Tool.byteArrayToHexString(localId));

        AtomicBoolean isSuccess = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        channelFuture.channel().closeFuture().addListener((ChannelFutureListener) future -> {
            boolean success = future.channel().attr(IS_SUCCESS).get();
            isSuccess.set(success);
            //System.out.println("get res " + remoteIp+":"+success);
            latch.countDown(); // 通知主线程
        });

        channelFuture.channel().closeFuture().sync(); // 等待 channel 关闭
        latch.await(); // 等待监听器执行完成

        //System.out.println("return res " + remoteIp+":"+isSuccess.get());
        return isSuccess.get();
    }


}
