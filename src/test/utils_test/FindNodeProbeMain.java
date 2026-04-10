package test.utils_test;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import myDiscover.message.MyFindNodeMessage;
import myDiscover.table.NodeId;
import org.tron.p2p.base.Constant;
import org.tron.p2p.discover.Node;
import org.tron.p2p.stats.TrafficStats;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class FindNodeProbeMain {

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);

        byte[] targetId = hexToBytes(config.targetIdHex);
        byte[] localIdBytes = randomNodeIdBytes();
        NodeId localNodeId = buildNodeId(localIdBytes);
        Node localNode = new Node(localIdBytes, config.fromIp, "", config.bindPort);

        List<InetSocketAddress> peers = readPeers(config.nodesFile);
        if (peers.isEmpty()) {
            System.err.println("nodes file is empty: " + config.nodesFile.toAbsolutePath());
            return;
        }

        Path outputFile = config.outputFile != null
                ? config.outputFile
                : Paths.get(System.getProperty("user.dir"),
                "findnode_result_" + nowForFileName() + ".txt");

        ProbeWriter writer = new ProbeWriter(outputFile);
        writer.line("=== FindNode Probe Started ===");
        writer.line("workDir      = " + Paths.get(System.getProperty("user.dir")).toAbsolutePath());
        writer.line("nodesFile    = " + config.nodesFile.toAbsolutePath());
        writer.line("outputFile   = " + outputFile.toAbsolutePath());
        writer.line("bindIp       = " + config.bindIp);
        writer.line("fromIp       = " + config.fromIp);
        writer.line("bindPort     = " + config.bindPort);
        writer.line("waitMs       = " + config.waitMs);
        writer.line("sendGapMs    = " + config.sendGapMs);
        writer.line("targetId     = " + bytesToHex(targetId));
        writer.line("localNodeId  = " + bytesToHex(localIdBytes));
        writer.line("peerCount    = " + peers.size());
        writer.line("");

        final Map<String, ProbeResult> results = new ConcurrentHashMap<String, ProbeResult>();
        final Set<String> expectedPeers = Collections.synchronizedSet(new LinkedHashSet<String>());
        for (InetSocketAddress peer : peers) {
            String key = addrKey(peer);
            expectedPeers.add(key);
            results.put(key, new ProbeResult(key));
        }

        final AtomicReference<MyMessageHandler> senderRef = new AtomicReference<MyMessageHandler>();

        MyNeighborsListener listener = new MyNeighborsListener() {
            @Override
            public void onResult(InetSocketAddress from, List<Node> nodes, boolean containsTarget) {
                String key = addrKey(from);
                ProbeResult result = results.get(key);
                if (result == null) {
                    result = new ProbeResult(key);
                    results.put(key, result);
                }

                result.responded = true;
                result.responsePackets++;
                if (containsTarget) {
                    result.containsTarget = true;
                }

                List<String> neighborIds = new ArrayList<String>();
                if (nodes != null) {
                    for (Node node : nodes) {
                        String nodeIdHex = safeNodeIdHex(node);
                        if (nodeIdHex != null) {
                            result.neighborIds.add(nodeIdHex);
                            neighborIds.add(nodeIdHex);
                        }
                    }
                }

                writer.line("[" + nowForLog() + "] RESPONSE from=" + key
                        + " packetIndex=" + result.responsePackets
                        + " nodeCount=" + (nodes == null ? 0 : nodes.size())
                        + " containsTarget=" + containsTarget);

                if (!neighborIds.isEmpty()) {
                    writer.line("  neighborIds=" + join(neighborIds, ","));
                }
            }
        };

        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            final MyEventHandler eventHandler =
                    new MyEventHandler(localNodeId, config.bindPort, targetId, listener);

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, false)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(TrafficStats.udp);
                            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                            ch.pipeline().addLast(new MyP2pPacketDecoder());
                            MyMessageHandler handler = new MyMessageHandler(ch, eventHandler);
                            eventHandler.setSender(handler);
                            senderRef.set(handler);
                            ch.pipeline().addLast(handler);
                        }
                    });

            NioDatagramChannel channel =
                    (NioDatagramChannel) bootstrap.bind(config.bindIp, config.bindPort).sync().channel();

            writer.line("[" + nowForLog() + "] UDP bound to " + config.bindIp + ":" + config.bindPort);
            writer.line("");

            MyMessageHandler sender = senderRef.get();
            if (sender == null) {
                throw new IllegalStateException("MyMessageHandler init failed");
            }

            for (InetSocketAddress peer : peers) {
                String key = addrKey(peer);
                ProbeResult result = results.get(key);
                result.sent = true;
                result.sendTime = System.currentTimeMillis();

                MyFindNodeMessage findNodeMessage = new MyFindNodeMessage(localNode, targetId);
                sender.accept(new MyUdpEvent(findNodeMessage, peer));

                writer.line("[" + nowForLog() + "] SEND to=" + key
                        + " targetId=" + bytesToHex(targetId));

                Thread.sleep(config.sendGapMs);
            }

            writer.line("");
            writer.line("[" + nowForLog() + "] waiting for responses ...");
            Thread.sleep(config.waitMs);

            writer.line("");
            writer.line("=== Summary ===");
            int respondedCount = 0;
            int hitCount = 0;

            List<String> orderedKeys = new ArrayList<String>(expectedPeers);
            Collections.sort(orderedKeys);

            for (String key : orderedKeys) {
                ProbeResult result = results.get(key);
                if (result != null && result.responded) {
                    respondedCount++;
                }
                if (result != null && result.containsTarget) {
                    hitCount++;
                }

                writer.line("peer=" + key
                        + " sent=" + (result != null && result.sent)
                        + " responded=" + (result != null && result.responded)
                        + " responsePackets=" + (result == null ? 0 : result.responsePackets)
                        + " containsTarget=" + (result != null && result.containsTarget)
                        + " neighborIdCount=" + (result == null ? 0 : result.neighborIds.size()));

                if (result != null && !result.neighborIds.isEmpty()) {
                    writer.line("  neighborIds=" + join(new ArrayList<String>(result.neighborIds), ","));
                }
            }

            writer.line("");
            writer.line("totalPeers=" + orderedKeys.size());
            writer.line("respondedPeers=" + respondedCount);
            writer.line("hitPeers=" + hitCount);
            writer.line("=== Finished ===");

            channel.close().sync();
        } finally {
            try {
                group.shutdownGracefully().sync();
            } catch (Exception ignored) {
            }
            writer.close();
        }

        System.out.println("done. output written to: " + outputFile.toAbsolutePath());
    }

    private static List<InetSocketAddress> readPeers(Path nodesFile) throws IOException {
        List<String> lines = Files.readAllLines(nodesFile, StandardCharsets.UTF_8);
        List<InetSocketAddress> peers = new ArrayList<InetSocketAddress>();

        for (String raw : lines) {
            if (raw == null) {
                continue;
            }
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            int idx = line.lastIndexOf(':');
            if (idx <= 0 || idx == line.length() - 1) {
                throw new IllegalArgumentException("bad peer line: " + line);
            }

            String host = line.substring(0, idx).trim();
            int port = Integer.parseInt(line.substring(idx + 1).trim());

            InetAddress inetAddress = InetAddress.getByName(host);
            peers.add(new InetSocketAddress(inetAddress, port));
        }

        return peers;
    }

    private static byte[] randomNodeIdBytes() {
        byte[] bytes = new byte[Constant.NODE_ID_LEN];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    private static NodeId buildNodeId(byte[] rawId) {
        if (rawId == null) {
            throw new IllegalArgumentException("rawId is null");
        }

        Byte[] boxed = new Byte[rawId.length];
        for (int i = 0; i < rawId.length; i++) {
            boxed[i] = rawId[i];
        }

        return new NodeId(boxed);
    }

    private static String safeNodeIdHex(Node node) {
        if (node == null) {
            return null;
        }

        try {
            Method m = node.getClass().getMethod("getId");
            Object value = m.invoke(node);
            if (value instanceof byte[]) {
                return bytesToHex((byte[]) value);
            }
        } catch (Exception ignored) {
        }

        try {
            Method m = node.getClass().getMethod("getHexId");
            Object value = m.invoke(node);
            if (value != null) {
                return String.valueOf(value);
            }
        } catch (Exception ignored) {
        }

        return null;
    }

    private static String addrKey(InetSocketAddress address) {
        if (address == null) {
            return "unknown";
        }
        InetAddress inetAddress = address.getAddress();
        String host = inetAddress == null ? address.getHostString() : inetAddress.getHostAddress();
        return host + ":" + address.getPort();
    }

    private static byte[] hexToBytes(String hex) {
        String s = normalizeHex(hex);
        if ((s.length() & 1) != 0) {
            throw new IllegalArgumentException("target-id hex length must be even");
        }

        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < s.length(); i += 2) {
            out[i / 2] = (byte) Integer.parseInt(s.substring(i, i + 2), 16);
        }
        return out;
    }

    private static String normalizeHex(String hex) {
        String s = hex.trim();
        if (s.startsWith("0x") || s.startsWith("0X")) {
            s = s.substring(2);
        }
        return s.replace(" ", "").toLowerCase();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static String join(List<String> values, String sep) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(sep);
            }
            sb.append(values.get(i));
        }
        return sb.toString();
    }

    private static String nowForFileName() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        sdf.setTimeZone(TimeZone.getDefault());
        return sdf.format(new Date());
    }

    private static String nowForLog() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getDefault());
        return sdf.format(new Date());
    }

    private static String detectLocalIpv4() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return "127.0.0.1";
    }

    private static final class Config {
        private Path nodesFile;
        private String targetIdHex;
        private String bindIp = "0.0.0.0";
        private String fromIp = detectLocalIpv4();
        private int bindPort = 18888;
        private long waitMs = 5000L;
        private long sendGapMs = 100L;
        private Path outputFile;

        static Config parse(String[] args) {
            Config c = new Config();
            Map<String, String> kv = parseArgs(args);

            if (!kv.containsKey("--target-id")) {
                printUsageAndExit();
            }

            c.targetIdHex = kv.get("--target-id");

            if (kv.containsKey("--nodes")) {
                c.nodesFile = Paths.get(kv.get("--nodes"));
            } else {
                c.nodesFile = Paths.get(System.getProperty("user.dir"), "nodes.txt");
            }

            if (kv.containsKey("--bind-ip")) {
                c.bindIp = kv.get("--bind-ip");
            }
            if (kv.containsKey("--from-ip")) {
                c.fromIp = kv.get("--from-ip");
            }
            if (kv.containsKey("--bind-port")) {
                c.bindPort = Integer.parseInt(kv.get("--bind-port"));
            }
            if (kv.containsKey("--wait-ms")) {
                c.waitMs = Long.parseLong(kv.get("--wait-ms"));
            }
            if (kv.containsKey("--send-gap-ms")) {
                c.sendGapMs = Long.parseLong(kv.get("--send-gap-ms"));
            }
            if (kv.containsKey("--output")) {
                c.outputFile = Paths.get(kv.get("--output"));
            }

            return c;
        }

        private static Map<String, String> parseArgs(String[] args) {
            Map<String, String> kv = new LinkedHashMap<String, String>();
            for (int i = 0; i < args.length; i++) {
                String k = args[i];
                if (!k.startsWith("--")) {
                    continue;
                }
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for " + k);
                }
                kv.put(k, args[++i]);
            }
            return kv;
        }

        private static void printUsageAndExit() {
            System.err.println("Usage:");
            System.err.println("  java -jar your.jar "
                    + "--target-id <hex> "
                    + "[--nodes nodes.txt] "
                    + "[--bind-ip 0.0.0.0] "
                    + "[--from-ip 192.168.1.10] "
                    + "[--bind-port 18888] "
                    + "[--wait-ms 5000] "
                    + "[--send-gap-ms 100] "
                    + "[--output result.txt]");
            System.exit(1);
        }
    }

    private static final class ProbeResult {
        final String peer;
        volatile boolean sent;
        volatile long sendTime;
        volatile boolean responded;
        volatile int responsePackets;
        volatile boolean containsTarget;
        final Set<String> neighborIds = Collections.synchronizedSet(new LinkedHashSet<String>());

        ProbeResult(String peer) {
            this.peer = peer;
        }
    }

    private static final class ProbeWriter {
        private final BufferedWriter writer;

        ProbeWriter(Path outputFile) throws IOException {
            this.writer = Files.newBufferedWriter(
                    outputFile,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        }

        synchronized void line(String s) {
            try {
                writer.write(s);
                writer.newLine();
                writer.flush();
                System.out.println(s);
            } catch (IOException e) {
                throw new RuntimeException("write output failed", e);
            }
        }

        synchronized void close() {
            try {
                writer.flush();
            } catch (Exception ignored) {
            }
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }
}