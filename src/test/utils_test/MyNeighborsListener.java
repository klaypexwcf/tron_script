package test.utils_test;

import org.tron.p2p.discover.Node;

import java.net.InetSocketAddress;
import java.util.List;

public interface MyNeighborsListener {
    void onResult(InetSocketAddress from, List<Node> nodes, boolean containsTarget);
}