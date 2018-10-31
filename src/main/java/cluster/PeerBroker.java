package cluster;

import central.Resources;
import common.Msg;
import common.Shared;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.Random;

/**
 * @author anpham
 * Local cluster or a leaf cluster
 */

public class PeerBroker {
    public static ZContext ctx;
    public static ZMQ.Socket pubSock;
    public static ZMQ.Socket routerSock;
    private static ZMQ.Socket subscriber;
    private static ZMQ.Socket dealer;
    private String name;

    public PeerBroker(String name) {
        this.name = name;
    }

    public static void main(String[] args) {
        PeerBroker self = new PeerBroker("Cl1");

        self.initializeGateway();

        self.initializeNodes(args);

//        while (!Thread.currentThread().isInterrupted()) {
        while (true) {
            //  Poll for activity, or 1 second timeout
//            ZMQ.PollItem items[] = {new ZMQ.PollItem(self.insertfe, ZMQ.Poller.POLLIN)};
            ZMQ.Poller poller = self.ctx.createPoller(3);
            poller.register(self.routerSock, ZMQ.Poller.POLLIN);
//            poller.register(self.pubSock, ZMQ.Poller.POLLIN);
            // two sockets to deal with Cloud Central
            poller.register(self.subscriber, ZMQ.Poller.POLLIN);
            poller.register(self.dealer, ZMQ.Poller.POLLIN);

            int rc = poller.poll(10 * 1000);
            if (rc == -1)
                break; //  Interrupted

            ZMsg result;

            //  Handle incoming status messages
            if (poller.pollin(0)) {
                result = ZMsg.recvMsg(self.routerSock);
//                System.out.println("BROKER: Receive request from client:\n" + result);
                // do sth and wait for response here
//                self.routerSock.send("Insert req RECEIVED: " + result);

                System.out.println("BROKER: Broadcast request from client:\n" + result);
//                self.pubSock.send("Broadcast Insert req received: " + result);
                result.send(pubSock,false);
                result.send(dealer);
            } else if (poller.pollin(1)) {
                // for published msg from central node
                result = ZMsg.recvMsg(self.subscriber);
                result.dump();

                // AnP: Redirect this msg to all nodes.
                result.send(pubSock);
//                self.pubSock.send(result);

            } else if (poller.pollin(2)) {
                // for handling requests from cloud
                result = ZMsg.recvMsg(self.dealer);

                try {
                    Msg m = new Msg(result);
                    byte[] msgContent = m.Command;
                    int msgLength = msgContent.length;
                    byte[] peerDst = Arrays.copyOfRange(msgContent, msgLength - 2, msgLength - 1);
                    byte[] response = Arrays.copyOfRange(msgContent, 0, msgContent.length - 3);

                    Resources.getResponseMessage(response, peerDst).send(routerSock);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        self.ctx.destroy();
    }

    /**
     * @param args numberOfNodes - nChunks - chunkSize - hammingThres - threshold C - numberOfGui
     */
    private void initializeNodes(String[] args) {
        int numberOfNodes = Integer.parseInt(args[0]);
        int nChunks = Integer.parseInt(args[1]);
        int chunkSize = Integer.parseInt(args[2]);
        int hammingThres = Integer.parseInt(args[3]);
        int cThres = Integer.parseInt(args[4]);
        int numberOfGui = Integer.parseInt(args[5]);

        int[] rand = new int[]{
                new Random().nextInt(numberOfNodes),
                new Random().nextInt(numberOfNodes),
                new Random().nextInt(numberOfNodes),
                new Random().nextInt(numberOfNodes)
        };

        for (int i = 0; i < numberOfNodes; i++) {
            boolean withGui = false;
            if (numberOfGui > 0) {
                // TODO: Improve randomness of node pick
                withGui = true;
                numberOfGui--;
            }
            new Thread(new PeerNode(this, nChunks, chunkSize, hammingThres, cThres, withGui, i)).start();
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

        //bindings....
        subscriber.connect(String.format(Shared.CENTRAL_ADDR, Shared.PUB_SUB_PROTOCOL, Shared.PUB_SUB_PORT));
        subscriber.subscribe("");
        //dealer.setIdentity("0913404k4822".getBytes());
        dealer.connect(String.format(Shared.CENTRAL_ADDR, Shared.ROUTER_DEALER_PROTOCOL, Shared.ROUTER_DEALER_PORT));
//                Resources.getInstance().ROUTER_DEALER_PROTOCOL,Resources.getInstance().ROUTER_DEALER_PORT));
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.err.println("DEALER is waiting a bit to connect to ROUTER... Please don't interrupt!");
        }
        System.out.println("finished Initializing DEALER");
    }

    public String getName() {
        return name;
    }
}
