package common;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class Shared {
    public static final String LOCAL_PUBLISH_SOCK = "ipc://%s-insertbe.ipc";
    public static final String LOCAL_ROUTER_SOCK = "ipc://%s-insertfe.ipc";

    public static final String CENTRAL_ADDR = "%s://localhost:%s";

    public static final int PUB_SUB_PORT = 5543;
    public static final int ROUTER_DEALER_PORT = 5544;
    public static final String PUB_SUB_PROTOCOL = "tcp";
    public static final String ROUTER_DEALER_PROTOCOL = "tcp";


    public static void sendQuery(byte[] q, ZMQ.Socket dealer) {
        ZMsg outgoing = new ZMsg();
        outgoing.add(q);
        outgoing.add("Q");
        outgoing.send(dealer);
    }

    public static void sendAdd(byte[] q, ZMQ.Socket sender) {
        getAddMessage(q, sender).send(sender);
    }

    public static void sendResponse(String resp, String clusterAddr, ZMQ.Socket sender) {
        getResponseMessage(resp.getBytes(), clusterAddr.getBytes()).send(sender);
    }

    public static ZMsg getQueryMessage(byte[] q, byte[] clusterAddr) {
        ZMsg outgoing = new ZMsg();
        outgoing.add(clusterAddr);
        outgoing.add(q);
        outgoing.add("Q");
        return outgoing;
    }

    public static ZMsg getAddMessage(byte[] q, ZMQ.Socket sender) {

        //ZMsg outgoing=ZMsg.newStringMsg("A");
        //        outgoing.add(new ZFrame("A"));
        ZMsg outgoing = new ZMsg();
        outgoing.add(sender.getIdentity());
        outgoing.add(q);
        outgoing.add("A");

        return outgoing;
    }

    public static ZMsg getResponseMessage(byte[] resp, byte[] clusterAddr) {
        ZMsg outgoing = ZMsg.newStringMsg("R");
        outgoing.push(resp);
        outgoing.push(clusterAddr);
        return outgoing;
    }
}
