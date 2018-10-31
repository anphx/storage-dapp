/**
 * @author: anpham
 * This class handle storing and searching mechanism inside a peer node
 * All requests will be directed to peer Broker for handling.
 * @param maxChunks: max number of chunks each peer node can store, default = 10
 * @param chunkSize: the size of each data chunk = X bits
 * @param hammingT: Hamming distance threshold, configurable
 * @param sumThreshold: Threshold C of position-wise summation.
 * <p>
 * storageArr:  an array of nChunks row, each row stores a Hashtable<address, data>
 */

package cluster;

import common.Shared;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import smile.math.distance.HammingDistance;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;


@SuppressWarnings("Since15")
public class PeerNode implements Runnable {
    public byte[] myID;
    private PeerBroker peerCluster;
    private ZContext ctx;
    private ZMQ.Socket subSock;
    private ZMQ.Socket dealerSock;
    private int maxChunks;
    private int chunkBits;
    private int chunkBytes;
    private float hammingT;
    private int sumThreshold;
    private Boolean isGui;
    private PeerNodeGui myGui;
    private Object[][] storageArr;

    public PeerNode(PeerBroker cluster, int nChunks, int chSize, int thresholdT, int C, boolean withGui, int index) {
        peerCluster = cluster;
        maxChunks = nChunks;
        chunkBits = chSize;
        chunkBytes = toByte(chunkBits);
        hammingT = thresholdT;
        isGui = withGui;
        sumThreshold = C;
        myID = String.valueOf(index).getBytes();

        initializeStorage();

        // Initialize context to use for the whole life cycle of a node
        ctx = new ZContext(1);
        connectTo(peerCluster.getName());
    }

    public void run() {
        if (isGui) {
            myGui = new PeerNodeGui(this);
            myGui.showup();
        }

        boolean done = false;

        while (!Thread.currentThread().isInterrupted()) {
            //  Poll for activity, or 1 second timeout
            ZMQ.Poller poller = ctx.createPoller(2);
            poller.register(subSock, ZMQ.Poller.POLLIN);
            poller.register(dealerSock, ZMQ.Poller.POLLIN);

            // Wait 10ms for a response, otherwise dismiss
            int rc = poller.poll(10 * 1000);
//            if (rc == -1)
//                break; //  Interrupted

            //  Handle incoming requests
            if (poller.pollin(0)) {
                // Receive broadcast msg
                ZMsg incomingMsg = ZMsg.recvMsg(subSock);
                System.out.println("Received BROADCAST msg from server:\n" + incomingMsg);
                handleResquest(incomingMsg);
            } else if (poller.pollin(1)) {
                // Handle response for any query request of this node
                return;
            }
        }
    }

    public void sendInsert(String input) {
//        ZMsg outgoing = ZMsg.newStringMsg("A");
//        outgoing.push(input.getBytes());
//        outgoing.send(dealerSock);
        System.out.println("PEER NODE: Req received " + input);
        Shared.sendAdd(input.getBytes(), dealerSock);
    }

    public void sendQuery(String input) {
        byte[] msgContent = concatByteArray(input.getBytes(), myID);
        System.out.println("PEER NODE: Send query received " + input);
        Shared.sendQuery(msgContent, dealerSock);
    }

    public void sendResponse(byte[] query, byte[] dst, String clusterAddr) {
        ZMsg outgoing = ZMsg.newStringMsg("R");
        byte[] content = concatByteArray(query, dst);
        outgoing.push(content);
        outgoing.push(clusterAddr);
        outgoing.send(dealerSock);
    }

    private byte[] concatByteArray(byte[] a, byte[] b) {
        byte[] content = new byte[a.length + b.length];
        System.arraycopy(a, 0, content, 0, a.length);
        System.arraycopy(b, 0, content, a.length, b.length);
        return content;
    }

//    public static void main(String[] args) {
//        //TODO: change for deployment
//        String name = args[0];
//        PeerNode self = new PeerNode(new PeerBroker("Cl1"), 10, 16, 3, false);
//        ZMQ.Socket insertfe = self.ctx.createSocket(ZMQ.REQ);
//        insertfe.connect(String.format("ipc://%s-insertfe.ipc", "Cl1"));
//
//        ZMQ.Socket insertbe = self.ctx.createSocket(ZMQ.SUB);
//        insertbe.subscribe("".getBytes());
//        insertbe.connect(String.format("ipc://%s-insertbe.ipc", "Cl1"));
//
//        System.out.println(name + " sent insert REQ to broker----");
//        insertfe.send(name + " want to insert sth.....");
//        boolean done = false;
//
////        while (!done) {
////            //  Poll for activity, or 1 second timeout
////            ZMQ.Poller poller = self.ctx.createPoller(2);
////            poller.register(insertfe, ZMQ.Poller.POLLIN);
////            poller.register(insertbe, ZMQ.Poller.POLLIN);
////
////            int rc = poller.poll(10 * 1000);
////            if (rc == -1)
////                break; //  Interrupted
////
////            //  Handle incoming status messages
////            if (poller.pollin(0)) {
////                String result = new String(insertfe.recv(0));
////                System.out.println("Server answer:\n" + result);
////            } else if (poller.pollin(1)) {
////                // Receive broadcast msg
////                String result = new String(insertbe.recv(0));
////                System.out.println("Server BROADCAST:\n" + result);
////            } else {
////                done = true;
////            }
////
////        }
//    }

    private int toByte(int xBits) {
        return Math.round(xBits / 8);
    }

    private void connectTo(String clusterName) {
        //  Subscribe to the insertbe of cluster for any inserting/searching req
        subSock = ctx.createSocket(ZMQ.SUB);
        subSock.subscribe("".getBytes());
        subSock.connect(String.format(Shared.LOCAL_PUBLISH_SOCK, clusterName));

        dealerSock = ctx.createSocket(ZMQ.DEALER);
        dealerSock.connect(String.format(Shared.LOCAL_ROUTER_SOCK, clusterName));
    }

    private void initializeStorage() {
        storageArr = new Object[maxChunks][];
        for (int i = 0; i < maxChunks; i++) {
            Object[] arr = new Object[3];

            arr[0] = randomize();
            arr[1] = new byte[chunkBits];
            // store max value in the data array
            arr[2] = 0;
            storageArr[i] = arr;
        }
    }

    private BitSet randomize() {
        Random random = new Random();
        BitSet bits = new BitSet(chunkBits);
        for (int i = 0; i < chunkBits; ++i)
            bits.set(i, random.nextBoolean());

        return bits;
    }

    private int doInsert(byte[] inputBytes) {
        // Convert str to bitset
//        BitSet inputSet = new BitSet(chunkBits);
        BitSet inputSet = BitSet.valueOf(inputBytes);
        if (inputSet.size() != chunkBits) return 0;

        // Find suitable memory
        for (int i = 0; i < storageArr.length; i++) {
            // Compare hamming distance of location addr and input data
            int addrDist = HammingDistance.d((BitSet) storageArr[i][0], inputSet);
            if (addrDist <= hammingT) {
                storageArr[i][1] = sumAt(inputSet, i);
            }
        }
        return 1;
    }

    private byte[] sumAt(BitSet input, int index) {
        int currMax = Integer.parseInt(storageArr[index][2].toString());
        byte[] curr = (byte[]) storageArr[index][1];

        if (currMax >= sumThreshold) return curr;
        if (input.size() != chunkBits) {
            System.out.println("Invalid format of input, should be " + chunkBits + " bits in size");
            return curr;
        }
        byte[] result = new byte[chunkBits];
        int max = 0;
        for (int i = 0; i < chunkBits; i++) {
            int val = input.get(i) ? 1 : -1;
            int temp = curr[i] + val;
            if (Math.abs(temp) > max) {
                max = Math.abs(temp);
            }
            result[i] = (byte) temp;
        }
        storageArr[index][2] = max;
        return result;
    }

    private byte[] sumOf(byte[] curr, byte[] data) {
        if (curr.length != data.length) return curr;
        byte[] result = new byte[chunkBits];

        for (int i = 0; i < chunkBits; i++) {
            result[i] = (byte) (curr[i] + data[i]);
        }
        return result;
    }

    private byte[] doMatch(byte[] query) {
        BitSet inputStr = BitSet.valueOf(query);

        if (inputStr.size() != chunkBits) return null;
        byte[] resultArr = new byte[chunkBits];
        for (int i = 0; i < storageArr.length; i++) {
            int addrDist = HammingDistance.d((BitSet) storageArr[i][0], inputStr);
            if (addrDist <= hammingT) {
                resultArr = sumOf(resultArr, (byte[]) storageArr[i][1]);
            }
        }
        return resultArr;
    }

    private void handleResquest(ZMsg msg) {
        String clusterSrc = msg.popString();
        byte[] msgContent = msg.pop().getData();
        char cmdType = msg.popString().charAt(0);
        String clusterAddr = msg.popString();
        switch (cmdType) {
            // ADD
            case 'A':
                doInsert(msgContent);
                break;
            case 'Q':
                // AnP: msg content has 2 parts: [query][node_addressx2B]
                // need to separate these parts here to process query only
                int msgLength = msgContent.length;
                byte[] peerDst = Arrays.copyOfRange(msgContent, msgLength - 2, msgLength - 1);
                byte[] query = Arrays.copyOfRange(msgContent, 0, msgContent.length - 3);
                sendResponse(doMatch(query), peerDst, clusterAddr);

                // TODO: Send back response
                break;
            case 'R':
//                doHandleResponse(msgContent);
                break;

        }
    }

//    public class RequestHandler implements Runnable {
//        private ZMsg msg;
//        private PeerNode myNode;
//
//        public RequestHandler(ZMsg incomingMsg, PeerNode node) {
//            msg = incomingMsg;
//            myNode = node;
//        }
//
//        public void run() {
//            String clusterSrc = msg.popString();
//            byte[] msgContent = msg.pop().getData();
//            char cmdType = msg.popString().charAt(0);
//            switch (cmdType) {
//                // ADD
//                case 'A':
//                    doInsert(msgContent);
//                    break;
//                case 'Q':
//                    // AnP: msg content has 2 parts: [query][node_addressx2B]
//                    // need to separate these parts here to process query only
//                    int msgLength = msgContent.length;
//                    byte[] peerDst = Arrays.copyOfRange(msgContent[msgLength-2, msgLength -1);
//                    byte[] query = Arrays.copyOfRange(msgContent, 0, msgContent.length - 3);
//                    myNode.sendResponse(doMatch(query), peerDst);
//                    // TODO: Send back response
//                    break;
//                case 'R':
//                    doHandleResponse(msgContent);
//                    break;
//
//            }
//
//            // Terminate after finishing handling requests
//            Thread.currentThread().interrupt();
//            return;
//        }
//
//        private void doHandleResponse(byte[] content) {
//
//        }
//    }

}
