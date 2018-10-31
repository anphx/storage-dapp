package common;

import central.Resources;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class Shared {
    public static final String LOCAL_PUBLISH_SOCK = "ipc://%s-insertbe.ipc";
    public static final String LOCAL_ROUTER_SOCK = "ipc://%s-insertfe.ipc";

    public static final String CENTRAL_ADDR = "%s://localhost:%s";

    public static final int PUB_SUB_PORT=5543;
    public static final int ROUTER_DEALER_PORT=5544;
    public static final String PUB_SUB_PROTOCOL="tcp";
    public static final String ROUTER_DEALER_PROTOCOL="tcp";


    public static void sendQuery(byte[] q, ZMQ.Socket dealer) {
//        ZMsg outgoing = ZMsg.newStringMsg("Q");
//        outgoing.push(q);
        Resources.getQueryMessage(q).send(dealer);
//        outgoing.send(dealer);
    }

    public static void sendAdd(byte[] q, ZMQ.Socket dealer) {
//        ZMsg outgoing = ZMsg.newStringMsg("A");
//        outgoing.push(q);
//        outgoing.send(dealer);
        Resources.getAddMessage(q).send(dealer);
    }

    public static void sendResponse(String resp, String clusterAddr, ZMQ.Socket dealer) {
//        ZMsg outgoing = ZMsg.newStringMsg("R");
//        outgoing.push("message");
//        outgoing.push("destination address");
//        outgoing.send(dealer);
        Resources.getResponseMessage(resp.getBytes(), clusterAddr.getBytes()).send(dealer);

    }
}
