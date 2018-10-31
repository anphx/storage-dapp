package common;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class Shared {
    public static final String LOCAL_PUBLISH_SOCK = "ipc://%s-insertbe.ipc";
    public static final String LOCAL_ROUTER_SOCK = "ipc://%s-insertfe.ipc";

    public static void sendQuery(byte[] q, ZMQ.Socket dealer) {
        ZMsg outgoing = ZMsg.newStringMsg("Q");
        outgoing.push(q);
        outgoing.send(dealer);
    }

    public static void sendAdd(byte[] q, ZMQ.Socket dealer) {
        ZMsg outgoing = ZMsg.newStringMsg("A");
        outgoing.push(q);
        outgoing.send(dealer);
    }

    public static void sendResponse(String resp, String clusterAddr, ZMQ.Socket dealer) {
        ZMsg outgoing = ZMsg.newStringMsg("R");
        outgoing.push("message");
        outgoing.push("destination address");
        outgoing.send(dealer);
    }

    public static void sendJoin(String address, ZMQ.Socket dealer) {
        ZMsg outgoing = ZMsg.newStringMsg("J");
        outgoing.push(address);
        outgoing.send(dealer);
    }
}
