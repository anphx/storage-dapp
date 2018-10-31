package cluster;

import common.Shared;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Random;

/**
 * @author anpham
 * Local cluster or a leaf cluster
 */

public class PeerBroker {
    public ZContext ctx;
    public ZMQ.Socket pubSock;
    public ZMQ.Socket routerSock;
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
            ZMQ.Poller poller = self.ctx.createPoller(1);
            poller.register(self.routerSock, ZMQ.Poller.POLLIN);
            int rc = poller.poll(10 * 1000);
            if (rc == -1)
                break; //  Interrupted

//            String result = "";

            //  Handle incoming status messages
            if (poller.pollin(0)) {
//                result = new String(self.routerSock.recv(0));
                ZMsg result = ZMsg.recvMsg(self.routerSock);
                System.out.println("BROKER: Receive request from client:\n" + result);
                // do sth and wait for response here
                self.routerSock.send("Insert req RECEIVED: " + result);

                System.out.println("BROKER: Broadcast request from client:\n" + result);
                self.pubSock.send("Broadcast Insert req received: " + result);
            }
        }
//        self.ctx.destroy();
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

        //  Bind search backend to endpoint
//        ZMQ.Socket searchbe = ctx.createSocket(ZMQ.PUB);
//        searchbe.bind(String.format("ipc://%s-searchbe.ipc", name));
    }

    public String getName() {
        return name;
    }
}
