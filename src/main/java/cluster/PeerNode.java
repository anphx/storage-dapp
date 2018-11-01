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

import common.Msg;
import common.Shared;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import smile.math.distance.HammingDistance;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Date;
import java.util.Random;


@SuppressWarnings("Since15")
public class PeerNode implements Runnable {
    public byte[] myID;
    String myQuery = "";
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
    private int queryCounter = 0;
    private int myIndex;
    private int totalNodes;
    private boolean canQuery = true;
    private byte[] queryResult = new byte[chunkBits];
    private byte[] prevResult;
    private int numberOfIteration;


    public PeerNode(PeerBroker cluster, int nChunks, int chSize, int thresholdT, int C, boolean withGui, int index, int total) {
        peerCluster = cluster;
        maxChunks = nChunks;
        chunkBits = chSize;
        chunkBytes = toByte(chunkBits);
        hammingT = thresholdT;
        isGui = withGui;
        sumThreshold = C;
        myIndex = index;
        totalNodes = total;
//        myID = String.valueOf(myIndex).getBytes();

        ByteBuffer dbuf = ByteBuffer.allocate(2);
        dbuf.putShort((short) index);
        myID = dbuf.array(); // { 0, 1 }

        initializeStorage();

        // Initialize context to use for the whole life cycle of a node
        ctx = new ZContext(1);
        connectTo(peerCluster.getName());
    }

    public int getID() {
        return myIndex;
    }

    public void run() {
        if (isGui) {
            String constraints = "" +
                    "Maximum storage offered: " + maxChunks + "\n" +
                    "Data chunk size: " + chunkBits + "(bits) or " + chunkBytes + " (bytes)";

            myGui = new PeerNodeGui(this);
            myGui.showup();
            myGui.printInfo(constraints);
        }
        ZMQ.Poller poller = ctx.createPoller(2);
        poller.register(subSock, ZMQ.Poller.POLLIN);
        poller.register(dealerSock, ZMQ.Poller.POLLIN);

        while (!Thread.currentThread().isInterrupted()) {
            //  Poll for activity, or 1 second timeout

            // Wait 10ms for a response, otherwise dismiss
            int rc = poller.poll(-1);
            if (rc == -1)
                break; //  Interrupted

            //  Handle incoming requests
            if (poller.pollin(0)) {
                // Receive broadcast msg
                ZMsg incomingMsg = ZMsg.recvMsg(subSock);
                //@AmiR
                //System.out.println("NODE-"+myIndex+": Received BROADCAST msg from cluster:\n" + incomingMsg);
                handleRequest(incomingMsg);
            } else if (poller.pollin(1)) {
                // Handle response R[data, dest]
                ZMsg incomingMsg = ZMsg.recvMsg(dealerSock);
                //@AmiR
                //System.out.println("NODE-"+myIndex+": received response\n" + incomingMsg);
                doHandleResponse(incomingMsg);

                return;
            }
        }
    }

    public void sendQuery(String input) {
        if (!canQuery) return;
//        byte[] msgContent = concatByteArray(input.getBytes(), myID);
        //@AmiR
        //System.out.println("NODE-"+myIndex+": SENDING QUERY: " + input);
        if (isGui) {
            myGui.printlnOut("Waiting for response from cluster broker...................... ");
        }

        // AnP: This is to mark the iteration of a query request
        // use this to trace all responses and send another query
        // turn on canQuery class only when this iteration is done
        numberOfIteration = 1;
        queryCounter = 0;
        myQuery = input;
        canQuery = false;

        Shared.sendQuery(input.getBytes(), dealerSock);
    }

    private void doHandleResponse(ZMsg incomingMsg) {
        String sender = incomingMsg.popString();
        byte[] content = incomingMsg.pop().getData();

        // Accumulate all responses here

        queryResult = sumOf(queryResult, content);
        queryCounter++;

        if (queryCounter >= totalNodes) {
            // AnP: All responses for query are received
            BitSet query = new BitSet(chunkBits);
            for (int i = 0; i < queryResult.length; i++) {
//               queryResult[i] = queryResult[i] < 0 ? (byte)0 : (byte)1;
                if (queryResult[i] >= 0) {
                    query.set(i);
                    queryResult[i] = 1;
                } else {
                    queryResult[i] = 0;
                }
            }

            if (isGui) {
                myGui.printlnOut("===> Found this after 1 iteration ====> " + query.toByteArray());
            }

            // Checking terminate condition -> start new iteration or stop
            if (prevResult != null) {
                int distance = HammingDistance.d(prevResult, queryResult);
                if (distance == 0) {
                    canQuery = true;

                    // AnP: Terminate with exact match
                    if (isGui) {
                        myGui.printlnOut("===> Searching TERMINATED with exact match!!!!");
                    }
                    return;
                }
                if (distance >= chunkBytes / 2) {
                    canQuery = true;

                    // AnP: Terminate for no reason
                    if (isGui) {
                        myGui.printlnOut("===> Searching TERMINATED!!!! No match found!!!!");
                    }
                    return;
                }
                if (numberOfIteration >= chunkBytes) {
                    canQuery = true;

                    // AnP: Terminate because you've done enough
                    if (isGui) {
                        myGui.printlnOut("===> Searching TERMINATED!!!! Found near match results!!!");
                    }
                    return;
                }
            }
            prevResult = queryResult;
            Shared.sendQuery(myQuery.getBytes(), dealerSock);
            numberOfIteration++;
        }
    }

    public void sendInsert(String input) {
//        ZMsg outgoing = ZMsg.newStringMsg("A");
//        outgoing.push(input.getBytes());
//        outgoing.send(dealerSock);
        //@AmiR
        //System.out.println("NODE-"+myIndex+": SENDING INSERT " + input);
        Shared.sendAdd(input.getBytes(), dealerSock);
    }

    private byte[] concatByteArray(byte[] a, byte[] b) {
        byte[] content = new byte[a.length + b.length];
        System.arraycopy(a, 0, content, 0, a.length);
        System.arraycopy(b, 0, content, a.length, b.length);
        return content;
    }

    private int toByte(int xBits) {
        return Math.round(xBits / 8);
    }

    private void connectTo(String clusterName) {
        //  Subscribe to the publish socket of cluster for any inserting/searching req
        subSock = ctx.createSocket(ZMQ.SUB);
        subSock.subscribe("".getBytes());
        subSock.connect(String.format(Shared.LOCAL_PUBLISH_SOCK, clusterName));

        dealerSock = ctx.createSocket(ZMQ.DEALER);
        dealerSock.setIdentity(myID);
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

    private byte[] randomize() {
        Random random = new Random();
//        BitSet bits = new BitSet(chunkBits);
        byte[] addresses = new byte[chunkBytes];
        new Random().nextBytes(addresses);
//        for (int i = 0; i < chunkBits; ++i)
//            bits.set(i, random.nextBoolean());

        return addresses;
    }

    private int doInsert(byte[] inputBytes) {
        System.out.println("NODE-" + myIndex + ": is adding chunk to my db...");
        // Convert str to bitset
//        BitSet inputSet = new BitSet(chunkBits);
        if (inputBytes.length != chunkBytes) return 0;
        BitSet inputBs = BitSet.valueOf(inputBytes);

        // Find suitable memory
        for (int i = 0; i < storageArr.length; i++) {
            // Compare hamming distance of location addr and input data
            int addrDist = HammingDistance.d((byte[]) storageArr[i][0], inputBytes);
            if (addrDist <= hammingT) {
                storageArr[i][1] = sumAt(inputBs, i);
            }
        }
        printStorageInfo();
        return 1;
    }

    private void printStorageInfo() {
        if (!isGui) return;
        myGui.printToStorage(new Date().toString() + ": ======> Printing storage info.........", true);
        myGui.printToStorage("Format: [location address], [data], [position-wise maximum value of data]", true);

        for (int i = 0; i < storageArr.length; i++) {
            // [address][data][max]
            myGui.printToStorage(i + ": [", false);
            myGui.printToStorage(storageArr[i][0] + "], [", false);
            myGui.printToStorage(storageArr[i][1].toString() + "], [", false);
            myGui.printToStorage(storageArr[i][2].toString() + "]", true);
        }
    }

    private byte[] sumAt(BitSet input, int index) {
        int currMax = Integer.parseInt(storageArr[index][2].toString());
        byte[] curr = (byte[]) storageArr[index][1];

        if (currMax >= sumThreshold) return curr;
//        if (input.size() != chunkBits) {
//            System.out.println("NODE-"+myIndex+"Invalid format of input, should be " + chunkBits + " bits in size");
//            return curr;
//        }
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
        storageArr[index][1] = result;
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
//        BitSet inputStr = BitSet.valueOf(query);
//        BitSet inputStr = new BitSet();

//        if (query.length != chunkBytes) return null;
        byte[] resultArr = new byte[chunkBits];
        for (int i = 0; i < storageArr.length; i++) {
            int addrDist = HammingDistance.d((byte[]) storageArr[i][0], query);
            if (addrDist <= hammingT) {
                resultArr = sumOf(resultArr, (byte[]) storageArr[i][1]);
            }
        }
        return resultArr;
    }

    private void handleRequest(ZMsg msg) {
        //@AmiR
        //System.out.print("NODE-"+myIndex+" RECEIVED THIS: ");
        try {
            char cmdType = msg.peekLast().toString().charAt(0);
            byte[] msgContent;
            Msg parsedMsg = new Msg(msg);

            switch (cmdType) {
                // ADD
                case 'A':
//                    String sender = msg.popString();
//                    msgContent = msg.pop().getData();
                    msgContent = parsedMsg.Command;
                    doInsert(msgContent);
                    break;
                case 'Q':
                    // AnP: msg content has 2 parts: [query][node_addressx2B]
                    // need to separate these parts here to process query only

//                    String clusterAddr = msg.peekFirst().toString();
//                    int msgLength = msgContent.length;
//                    byte[] peerDst = Arrays.copyOfRange(msgContent, msgLength - 2, msgLength - 1);
//                    byte[] query = Arrays.copyOfRange(msgContent, 0, msgContent.length - 2);

//                    msgContent = msg.pop().getData();
                    msgContent = parsedMsg.Command;
                    System.out.println("NODE-" + myIndex + ": HANDLING QUERY: " + new String(msgContent));
//                    sendResponse(doMatch(msgContent), dst.getBytes(), dst.getBytes());
                    sendResponse(doMatch(msgContent), parsedMsg.Source.getBytes(), parsedMsg.Source.getBytes());

                    break;
                case 'R':
                    // AnP: Don't do anything here because we only handle direct response
                    //@AmiR
                    //System.out.println("NODE-"+myIndex+": HANDLING A RESPONSE.. ");

                    //doHandleResponse(msgContent);
                    break;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendResponse(byte[] resp, byte[] dst, byte[] clusterAddr) {
//        byte[] content = concatByteArray(resp, dst);
        System.out.println("NODE-" + myIndex + ": SENDING RESPONSE " + resp);
        Shared.getResponseMessage(resp, clusterAddr).send(dealerSock);
    }
}
