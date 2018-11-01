package common;

import central.Resources;
import org.zeromq.ZFrame;
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
//        Resources.getQueryMessage(q, clusterAddr).send(dealer);

        ZMsg outgoing = new ZMsg();
        //outgoing.add(clusterAddr);
        outgoing.add(q);
        outgoing.add("Q");
        outgoing.send(dealer);
    }

    public static void sendAdd(byte[] q, ZMQ.Socket dealer) {
//        ZMsg outgoing = ZMsg.newStringMsg("A");
//        outgoing.push(q);
//        outgoing.send(dealer);
        getAddMessage(q).send(dealer);
    }

    public static void sendResponse(String resp, String clusterAddr, ZMQ.Socket dealer) {
//        ZMsg outgoing = ZMsg.newStringMsg("R");
//        outgoing.push("message");
//        outgoing.push("destination address");
//        outgoing.send(dealer);
        getResponseMessage(resp.getBytes(), clusterAddr.getBytes()).send(dealer);

    }

    public static ZMsg getQueryMessage(byte[] q, byte[] clusterAddr) {
        ZMsg outgoing = new ZMsg();
        outgoing.add(clusterAddr);
        outgoing.add(q);
        outgoing.add("Q");
        return outgoing;
        //outgoing.send(dealer);
    }

    public static ZMsg getAddMessage(byte[] q) {

        //ZMsg outgoing=ZMsg.newStringMsg("A");
        ZMsg outgoing = new ZMsg();
        //outgoing.add(new ZFrame("A"));

        //ZFrame zf = new ZFrame("k");
        outgoing.add(q);
//        outgoing.add(new ZFrame("A"));
        outgoing.add("A");

        return outgoing;
        //outgoing.send(dealer);

    }

    public static ZMsg getResponseMessage(byte[] resp, byte[] clusterAddr) {
        ZMsg outgoing = ZMsg.newStringMsg("R");
        outgoing.push(resp);
        outgoing.push(clusterAddr);
        return outgoing;
        //outgoing.send(dealer);

    }
}
