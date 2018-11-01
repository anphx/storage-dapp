/**
 * @author anpham
 * Local cluster or a leaf cluster
 */

package cluster;

import common.Msg;
import common.Shared;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import java.util.Random;

public class PeerBroker {
    public static ZContext ctx;
    public static ZMQ.Socket pubSock;
    public static ZMQ.Socket routerSock;
    private static ZMQ.Socket subscriber;
    private static ZMQ.Socket dealer;
    private String name;

    private int numOfNodes;
//    private int totalNodes; // S=P*N -> P is the number of clusters


    public PeerBroker(String name) {
        this.name = name;
    }

    public static void main(String[] args) {
        PeerBroker self = new PeerBroker("Cl1");

        self.initializeGateway();

        self.initializeNodes(args);
        ZMQ.Poller poller = self.ctx.createPoller(3);
        poller.register(self.routerSock, ZMQ.Poller.POLLIN);
        poller.register(self.subscriber, ZMQ.Poller.POLLIN);
        poller.register(self.dealer, ZMQ.Poller.POLLIN);

        while (true) {
            //  Poll for activity, or 1 second timeout


            int rc = poller.poll(  -1);
            if (rc == -1)
                break; //  Interrupted

            ZMsg result;

            if (poller.pollin(0)) {
                //  Handle incoming status messages from Router socket

                result = ZMsg.recvMsg(self.routerSock);

                try {
                    result.send(self.dealer, false);
//                    System.out.println("BROKER: Receive SUBSCRIPTION from cloud: " + result);

                    if (result.peekLast().toString().charAt(0) != 'R') {
//                        System.out.println("BROKER: PUBLISH");
                        result.send(self.pubSock);
                    } else {
//                        System.out.println("BROKER: FORWARD RESPONSE MSG TO ORIGINAL NODE");
                        Msg msg = new Msg(result);
//                        msg.dump();
                        Shared.getResponseMessage(msg.Command, msg.Destination.getBytes()).send(self.routerSock);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (poller.pollin(1)) {
                // for published msg from central node
                result = ZMsg.recvMsg(self.subscriber);
//                result.dump();

                // AnP: Redirect this msg to all nodes.
                result.send(self.pubSock);

            } else if (poller.pollin(2)) {
                // for direct requests from cloud
                result = ZMsg.recvMsg(self.dealer);
//                System.out.println("BROKER: EXCLUSIVE RESPONSE: " + result);

                try {
                    Msg m = new Msg(result);
//                    m.dump();

//                    byte[] msgContent = m.Command;

//                    ZMsg toSend = Shared.getResponseMessage(msgContent, m.Destination.getBytes());
//                            .send(self.routerSock);
                    self.routerSock.sendMore(m.Destination);
                    self.routerSock.sendMore(m.Command);
                    self.routerSock.send("R");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        self.ctx.destroy();
    }

    /**
     * @param args numOfNodes - nChunks - chunkSize - hammingThres - threshold C - numberOfGui - numberOfCluster
     */
    private void initializeNodes(String[] args) {
        numOfNodes = Integer.parseInt(args[0]);
        int nChunks = Integer.parseInt(args[1]);
        int chunkSize = Integer.parseInt(args[2]);
        int hammingThres = Integer.parseInt(args[3]);
        int cThres = Integer.parseInt(args[4]);
        int numOfGui = Integer.parseInt(args[5]);
        long numOfCluster = Integer.parseInt(args[6]);

        for (int i = 0; i < numOfNodes; i++) {
            boolean withGui = false;
            if (numOfGui > 0) {
                // TODO: Improve randomness of node pick
                withGui = true;
                numOfGui--;
            }
            new Thread(new PeerNode(this, nChunks, chunkSize, hammingThres, cThres, withGui, i, numOfCluster*numOfNodes)).start();
        }
    }

    public void initializeGateway() {
        ctx = new ZContext();

        // Socket to talk to peer node
        routerSock = ctx.createSocket(ZMQ.ROUTER);
        routerSock.bind(String.format(Shared.LOCAL_ROUTER_SOCK, name));

        //  Bind insert backend to endpoint
        pubSock = ctx.createSocket(ZMQ.PUB);
        pubSock.bind(String.format(Shared.LOCAL_PUBLISH_SOCK, name));

//        context=new ZContext(1);
        subscriber = ctx.createSocket(ZMQ.SUB);
        dealer = ctx.createSocket(ZMQ.DEALER);
        byte[] identity = new byte[2];
        new Random().nextBytes(identity);
//        dealer.setIdentity(identity);

        //bindings....
        subscriber.connect(String.format(Shared.CENTRAL_ADDR, Shared.PUB_SUB_PROTOCOL, Shared.PUB_SUB_PORT));
        subscriber.subscribe("");
        dealer.connect(String.format(Shared.CENTRAL_ADDR, Shared.ROUTER_DEALER_PROTOCOL, Shared.ROUTER_DEALER_PORT));
//                Resources.getInstance().ROUTER_DEALER_PROTOCOL,Resources.getInstance().ROUTER_DEALER_PORT));
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.err.println("DEALER is waiting a bit to connect to ROUTER... Please don't interrupt!");
        }
        System.out.println("BROKER: finished Initializing");
    }

    public String getName() {
        return name;
    }
}
