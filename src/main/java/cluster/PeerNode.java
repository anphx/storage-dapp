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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Random;


@SuppressWarnings("Since15")
public class PeerNode implements Runnable {
    public byte[] myID;
    public String myQuery = "";
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
    private long totalNodes;
    private boolean canQuery = true;
    private byte[] queryResult;
    private byte[] prevResult = null;
    private int numOfIteration;


    public PeerNode(PeerBroker cluster, int nChunks, int chSize, int thresholdT, int C, boolean withGui, int index, long total) {
        peerCluster = cluster;
        maxChunks = nChunks;
        chunkBits = chSize;
        chunkBytes = toByte(chunkBits);
        hammingT = thresholdT;
        isGui = withGui;
        sumThreshold = C;
        myIndex = index;
        totalNodes = total;

        ByteBuffer dbuf = ByteBuffer.allocate(2);
        dbuf.putShort((short)(index + 1));
        myID = dbuf.array(); // { 0, 1 }

        initializeStorage();

        // Initialize context to use for the whole life cycle of a node
        ctx = new ZContext(1);
        connectTo(peerCluster.getName());
//        System.out.println("NODE-" + myIndex + "-identity: " + new String(dealerSock.getIdentity()));
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
            }
        }
    }

    public void sendQuery(String input) {
        if (!canQuery) return;
        //@AmiR
        //System.out.println("NODE-"+myIndex+": SENDING QUERY: " + input);
        if (isGui) {
            myGui.printlnOut("Waiting for response from cluster broker...................... ");
        }

        // AnP: This is to mark the iteration of a query request
        // use this to trace all responses and send another query
        // turn on canQuery class only when this iteration is done
        numOfIteration = 1;
        queryResult = new byte[chunkBits];
        prevResult = null;
        queryCounter = 0;
        myQuery = input;
        canQuery = false;

        Shared.sendQuery(input.getBytes(), dealerSock);
    }

    private void doHandleResponse(ZMsg incomingMsg) {
//        System.out.println("NODE-" + myIndex + ": HANDLING RESPONSE!!!!!");
//        incomingMsg.dump();
        if (canQuery) return;

//        String sender = incomingMsg.popString();
        byte[] content = incomingMsg.pop().getData();

        // Accumulate all responses here

        queryResult = sumOf(queryResult, content);
        queryCounter++;

        if (queryCounter >= totalNodes) {
            queryCounter = 0;

            // AnP: All responses for query are received
            BitSet query = new BitSet(chunkBits);
            for (int i = 0; i < queryResult.length; i++) {
                if (queryResult[i] >= 0) {
                    query.set(i);
                    queryResult[i] = 1;
                } else {
                    queryResult[i] = 0;
                }
            }

            if (isGui) {
                myGui.printlnOut("===> Found this after " + numOfIteration + " iteration ====> " + new String(query.toByteArray()));
            }

            // Checking terminate condition -> start new iteration or stop
            if (prevResult != null) {
                int distance = HammingDistance.d(prevResult, queryResult);
//                int distance = calcHamming(prevResult, queryResult);

                if (distance == 0) {
                    canQuery = true;

                    // AnP: Terminate with exact match
                    if (isGui) {
                        myGui.printlnOut("===> Searching TERMINATED  match!!!!");
                    }
                    return;
                } else if (distance >= chunkBytes / 2) {
                    canQuery = true;

                    // AnP: Terminate for no reason
                    if (isGui) {
                        myGui.printlnOut("===> Searching TERMINATED!!!! No match found!!!!");
                    }
                    return;
                } else if (numOfIteration >= chunkBits / 2) {
                    canQuery = true;

                    // AnP: Terminate because you've done enough
                    if (isGui) {
                        myGui.printlnOut("===> Searching TERMINATED!!!! Found near match results!!!");
                    }
                    return;
                } else {
                    sendNextQuery(query);
                }
            } else {
                sendNextQuery(query);
            }
        }
    }

    private void sendNextQuery(BitSet query) {
        prevResult = new byte[chunkBits];
        prevResult = queryResult.clone();
//        System.arraycopy(queryResult, 0, prevResult, 0, queryResult.length);
        Shared.sendQuery(query.toByteArray(), dealerSock);
        queryResult = new byte[chunkBits];
        numOfIteration++;
    }

//    private int calcHamming(byte[] a, byte[] b) {
//        if (a.length != b.length) throw new IllegalArgumentException("Size mismatch");
//        int dist = 0;
//        for (int i = 0; i < a.length; i++) {
//            if (a[i] != b[i]) {
//                dist++;
//            }
//        }
//        return dist;
//    }

    public void sendInsert(String input) {
        //@AmiR
        //System.out.println("NODE-"+myIndex+": SENDING INSERT " + input);
        Shared.sendAdd(input.getBytes(), dealerSock);
    }

//    private byte[] concatByteArray(byte[] a, byte[] b) {
//        byte[] content = new byte[a.length + b.length];
//        System.arraycopy(a, 0, content, 0, a.length);
//        System.arraycopy(b, 0, content, a.length, b.length);
//        return content;
//    }

    private int toByte(int xBits) {
        return Math.round(xBits / 8);
    }

    private void connectTo(String clusterName) {
        //  Subscribe to the publish socket of cluster for any inserting/searching req
        subSock = ctx.createSocket(ZMQ.SUB);
        subSock.subscribe("".getBytes());
        subSock.connect(String.format(Shared.LOCAL_PUBLISH_SOCK, clusterName));

        dealerSock = ctx.createSocket(ZMQ.DEALER);
        dealerSock.setIdentity(String.valueOf(myIndex).getBytes(ZMQ.CHARSET));
        dealerSock.connect(String.format(Shared.LOCAL_ROUTER_SOCK, clusterName));
//        dealerSock.send(" dsds");
//        System.out.println("HEEEEEEEEY THIS IS MY IDENTITY:"+new String(dealerSock.getIdentity()));
    }

    private void initializeStorage() {
        storageArr = new Object[maxChunks][];
        for (int i = 0; i < maxChunks; i++) {
            Object[] arr = new Object[3];

            arr[0] = randomize(); //Bitset
            arr[1] = new byte[chunkBits];
            // store max value in the data array
            arr[2] = new Integer(0);
            storageArr[i] = arr;
        }
    }

    private BitSet randomize() {
//        Random random = new Random();
//        byte[] addresses = new byte[chunkBytes];
//
//        random.nextBytes(addresses);
//
//        return addresses;

        Random rand = new Random();
        BitSet bs = new BitSet(chunkBits);
        for (int i=0; i<chunkBits; i++) {
            bs.set(i, rand.nextBoolean());
        }
        return bs;
    }

    private int doInsert(byte[] inputBytes) {
       // System.out.println("NODE-" + myIndex + ": is adding chunk to my db...");
        if (inputBytes.length != chunkBytes) return 0;
        BitSet inputBs = BitSet.valueOf(inputBytes);

        // Find suitable memory
        for (int i = 0; i < storageArr.length; i++) {
            // Compare hamming distance of location addr and input data
            //System.out.println("Amirrrrrrr: size of the bitset is: "+BitSet.valueOf((byte[])storageArr[i][0]).size() );
            int addrDist = HammingDistance.d((BitSet)storageArr[i][0], inputBs);
            if (addrDist <= hammingT) {
                System.out.println("*****Storing in my db******");
                storageArr[i][1] = sumAt(inputBs, i);
            }
        }
//        printStorageInfo();
        return 1;
    }


    private byte[] sumAt(BitSet input, int index) {
        int currMax = Integer.parseInt(storageArr[index][2].toString());
        byte[] curr = (byte[]) storageArr[index][1];

        if (currMax >= sumThreshold) return curr;
        byte[] result = new byte[chunkBits];
        int max = 0;

        for (int i = 0; i < chunkBits; i++) {
            int val = input.get(i) ? 1 : -1;
            int temp = curr[i] + val;
            if (Math.abs(temp) > max) {
                max = Math.abs(temp);
            }
            result[i] = (byte)temp;
        }
//        storageArr[index][1] = result;
        storageArr[index][2] = max;
        return result;
    }

    private void printStorageInfo() {
        if (!isGui) return;
        myGui.printToStorage(new Date().toString() + ": ======> Printing storage info.........", true);
        myGui.printToStorage("Format: [location address], [data], [position-wise maximum value of data]", true);

        for (int i = 0; i < storageArr.length; i++) {
            // [address][data][max]
            myGui.printToStorage(i + ": [", false);
            myGui.printToStorage(new String(((BitSet)storageArr[i][0]).toByteArray()) + "], [", false);
            myGui.printToStorage(storageArr[i][1] + "], [", false);
            myGui.printToStorage(storageArr[i][2] + "]", true);
        }
    }

    private byte[] sumOf(byte[] curr, byte[] data) {
        if (curr == null || data == null || curr.length != data.length) return curr;
        byte[] result = new byte[chunkBits];

        for (int i = 0; i < chunkBits; i++) {
            result[i] = (byte) (curr[i] + data[i]);
        }
        return result;
    }

    private byte[] doMatch(byte[] query) {
        byte[] resultArr = new byte[chunkBits];
        if (query.length != chunkBytes) return resultArr;

        try {
            for (int i = 0; i < storageArr.length; i++) {
//                int addrDist = HammingDistance.d((byte[]) storageArr[i][0], query);
                int addrDist = HammingDistance.d((BitSet)storageArr[i][0], BitSet.valueOf(query));

                if (addrDist <= hammingT) {
                    resultArr = sumOf(resultArr, (byte[]) storageArr[i][1]);
                }
            }
        } catch (IllegalArgumentException ex) {
            myGui.printlnOut("<==== EXCEPTION " + ex.getMessage());
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
                    msgContent = parsedMsg.Command;
                    System.out.println("NODE-" + myIndex + ": HANDLING INSERT: " + new String(msgContent));

                    doInsert(msgContent);
                    break;
                case 'Q':
                    // AnP: msg content has 2 parts: [query][node_addressx2B]
                    // need to separate these parts here to process query only
                    msgContent = parsedMsg.Command;
//                    System.out.println("NODE-" + myIndex + ": HANDLING QUERY: " + new String(msgContent));

                    sendResponse(doMatch(msgContent), parsedMsg.Source.getBytes(), parsedMsg.Source.getBytes());

                    break;
                case 'R':
                    // AnP: Don't do anything here because we only handle direct response
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendResponse(byte[] resp, byte[] dst, byte[] clusterAddr) {
//        System.out.println("NODE-" + myIndex + ": SENDING RESPONSE " + resp);
        ZMsg msg = Shared.getResponseMessage(resp, clusterAddr);
//        msg.dump();
        msg.send(dealerSock);
//        System.out.println("NODE-" + myIndex + ": SENT!!!!!");
    }
}
