import java.io.*;
import java.lang.Thread;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;

public class Node implements Runnable {
    public int nodeNum; // number of node
    public int nodeID; // node index
    public String hostName; // hostname dc01
    public int perActive; // PerActive
    public int minSendDelay;
    public int snapShotDelay;
    public int maxNumber;
    public int port;
    public ArrayList<HashMap<String, String>> NodeInfoList;
    public ArrayList<Integer> outgoingNodeList;
    public String status; // passive or acitve
    public int appMsgSent; // application message sent
    public boolean passived; // mark the node has already became passive
    public boolean index0StartWait; // use to wait other node started
    public int parentID = -1; // parent id in the spanning tree
    public ArrayList<Integer> children = new ArrayList<>(); // children in the spanning tree
    public int[] timestamp_array;
    //--------------------------------
    public int receiveMarkNum; // leaf node will use it
    public int receiveConvergeNum; // root node will use it
    public String statusBuffer; // record when receive MARKER message
    public int[] timestampBuffer; // record when receive MARKER message
    public HashMap<Integer, String> statusCollection = new HashMap<>(); // record when receive CONVERGECAST message
    public HashMap<Integer, int[]> timestampCollection = new HashMap<>(); // record when receive CONVERGECAST message
    public boolean CLStarted;
    public boolean ifMAPStop;
    public boolean terminate;

    public Node() throws UnknownHostException {
        ConfigReader config = new ConfigReader();
        this.nodeNum = config.nodeNum;
        timestamp_array = new int[this.nodeNum];
        this.nodeID = config.myNodeIndex;
        this.hostName = config.myHostName;
        this.perActive = config.perActive;
        this.minSendDelay = config.minSendDelay;
        this.snapShotDelay = config.snapShotDelay;
        this.maxNumber = config.maxNumber;
        this.port = config.getPort();
        this.NodeInfoList = config.NodeInfoList;
        this.outgoingNodeList = config.outgoingNodeList;
        if (this.nodeID == 0) this.status = "active";
        else this.status = "passive";
        this.index0StartWait = true;
        this.appMsgSent = 0;
        this.passived = false;
        //------------------------
        this.receiveMarkNum = 0;
        this.receiveConvergeNum = 0;
        this.statusBuffer = "";
        this.timestampBuffer = new int[this.nodeNum];
        this.CLStarted = false;
        this.ifMAPStop = false;
        this.terminate = false;
    }

    public void makeSpanningTree() {
        broadcast("search");
    }

    private boolean ifMAPStop(HashMap<Integer, String> statusCollection) {
        for (HashMap.Entry<Integer, String> entry : statusCollection.entrySet()) {
            if (entry.getValue().equals("active")) return false;
        }
        return true;
    }

    public void outputFile(HashMap<Integer, int[]> timestampCollection) {
        System.out.println("sssssssnnnnnnaaaaaappppppssssssshhhhoooooottttttt");
        for (Map.Entry<Integer, int[]> entry : timestampCollection.entrySet()) {
            int NodeID = entry.getKey();
            int[] timestamp_array = entry.getValue();
            try {
                File file = new File("config-" + NodeID + ".out");
                if (!file.exists()) {
                    file.createNewFile();
                }
                FileWriter fileWriter = new FileWriter(file.getName(), true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                for (int each : timestamp_array) {
                    bufferedWriter.write(each + " ");
                }
                bufferedWriter.write("\n");
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void broadcastChildren(String msgType) {
        for (int child : this.children) {
            String broadcastChildren_hostname = this.NodeInfoList.get(child).get("hostname");
            int broadcastChildren_port = Integer.parseInt(this.NodeInfoList.get(child).get("port"));
            Message msg = new Message.MessageBuilder()
                    .from(this.nodeID)
                    .to(child)
                    .type(msgType)
                    .build();
            msg.sendMsg(msg, broadcastChildren_hostname + ".utdallas.edu", broadcastChildren_port);
        }
    }

    public void broadcast(String msgType) {
        for (int outgoingNode : this.outgoingNodeList) {
            if (outgoingNode != 0) {
                String broadcast_hostname = this.NodeInfoList.get(outgoingNode).get("hostname");
                int broadcast_port = Integer.parseInt(this.NodeInfoList.get(outgoingNode).get("port"));
                Message Msg = new Message.MessageBuilder()
                        .from(this.nodeID)
                        .to(outgoingNode)
                        .type(msgType)
                        .build();
                Msg.sendMsg(Msg, broadcast_hostname + ".utdallas.edu", broadcast_port);
            }
        }
    }

    public void listen() throws IOException {
        Thread listener = new NodeListener(this, this.port);
        listener.start();
        System.out.println("this listener has already started.");
    }

    public void sendAppMsg() throws IOException, InterruptedException {
        Thread appMsgSender = new AppMsgSender(this, this.outgoingNodeList);
        appMsgSender.start();
        System.out.println("this appMsgSender has started.");
    }

    public void chandyLamport() {
        ChandyLamport chandyLamport = new ChandyLamport(this);
        Thread chandyLamport_thread = new Thread(chandyLamport);
        chandyLamport_thread.start();
        System.out.println("Chandy Lamport protocol has already started");
    }

    public void receiveMsg(Message msg) throws InterruptedException {
        switch (msg.getType()) {
            case "application":
                System.out.println("> > > > > > > > > > > > receive an app message from " + msg.getSender() + " status now: " + this.status);
                // update timestamp
                for (int i = 0; i < this.nodeNum; i++) {
                    this.timestamp_array[i] = Math.max(this.timestamp_array[i], msg.getTimestamp_array()[i]);
                }
                this.timestamp_array[this.nodeID] += 1;
                if (this.appMsgSent < this.maxNumber && !this.status.equals("active")) {
                    System.out.println("appMsgSent less than maxNumber, change status to active");
                    this.status = "active";
                }
                break;
            case "accept":
                System.out.println("******* receive accept message from " + msg.getSender() + "add it in children list. My " + this.nodeID + "children now are: ");
                this.children.add(msg.getSender());
                for (int child : this.children) System.out.print(child + ", ");
                break;
            case "search":
                System.out.println("***** receive search message from " + msg.getSender());
                if (this.parentID == -1) {
                    this.parentID = msg.getSender();

                    // send accept message to my parent
                    System.out.println("*** I'm " + this.nodeID + " set my parent is " + this.parentID + " and send accept message to my parent and broadcast search message to outgoing edge...");
                    String parent_hostname = this.NodeInfoList.get(this.parentID).get("hostname");
                    int parent_port = Integer.parseInt(this.NodeInfoList.get(this.parentID).get("port"));
                    Message acceptMsg = new Message.MessageBuilder()
                            .from(this.nodeID)
                            .to(this.parentID)
                            .type("accept")
                            .build();
                    acceptMsg.sendMsg(acceptMsg, parent_hostname + ".utdallas.edu", parent_port);
                    Thread.sleep(1000);

                    // broadcast search message to my outgoing node
                    broadcast("search");
                }
                break;
            case "MARKER":
                System.out.println("MMMMMMMMMMMMM receive MARKER message from " + msg.getSender());
                this.receiveMarkNum += 1;
                if (!this.CLStarted) {
                    this.statusBuffer = this.status;
                    this.timestampBuffer = this.timestamp_array;
                    this.broadcast("MARKER");
                    this.CLStarted = true;
                }
                // leaf node
                if (this.children.size() == 0 && this.receiveMarkNum == this.outgoingNodeList.size()) {
                    // convergecast start
                    String hostname = this.NodeInfoList.get(this.parentID).get("hostname");
                    int port = Integer.parseInt(this.NodeInfoList.get(this.parentID).get("port"));
                    HashMap<Integer, String> temp_statusCollection = new HashMap<>();
                    HashMap<Integer, int[]> temp_timestampCollection = new HashMap<>();
                    temp_statusCollection.put(this.nodeID, this.statusBuffer);
                    temp_timestampCollection.put(this.nodeID, this.timestampBuffer);
                    Message Msg = new Message.MessageBuilder()
                            .from(this.nodeID)
                            .to(this.parentID)
                            .type("CONVERGECAST")
                            .statusCollection(temp_statusCollection)
                            .timestampCollection(temp_timestampCollection)
                            .build();
                    Msg.sendMsg(Msg, hostname + ".utdallas.edu", port);
                    this.receiveMarkNum = 0;
                    this.CLStarted = false;
                }
                break;
            case "CONVERGECAST":
                System.out.println("CCCCCCCCCCCCCC receive CONVERGECAST message from " + msg.getSender());
                this.receiveConvergeNum += 1;
                HashMap<Integer, String> msg_statusCollection = msg.getStatusCollection();
                HashMap<Integer, int[]> msg_timestampCollection = msg.getTimestampCollection();
                this.statusCollection.put(this.nodeID, this.statusBuffer);
                this.timestampCollection.put(this.nodeID, this.timestampBuffer);
                this.statusCollection.putAll(msg_statusCollection);
                this.timestampCollection.putAll(msg_timestampCollection);
                if (this.receiveConvergeNum == this.children.size()) {
                    if (this.nodeID == 0) {
                        if (ifMAPStop(this.statusCollection)) {
                            System.out.println("-------------------------set ifMapstop = true---------");
                            broadcastChildren("TERMINATE");
                            this.ifMAPStop = true;
                        }
                        outputFile(this.timestampCollection);
                    } else {
                        String hostname = this.NodeInfoList.get(this.parentID).get("hostname");
                        int port = Integer.parseInt(this.NodeInfoList.get(this.parentID).get("port"));
                        Message Msg = new Message.MessageBuilder()
                                .from(this.nodeID)
                                .to(this.parentID)
                                .type("CONVERGECAST")
                                .statusCollection(this.statusCollection)
                                .timestampCollection(this.timestampCollection)
                                .build();
                        Msg.sendMsg(Msg, hostname + ".utdallas.edu", port);
                    }
                    this.CLStarted = false;
                    this.statusCollection.clear();
                    this.timestampCollection.clear();
                    this.receiveConvergeNum = 0;
                }
                break;
            case "TERMINATE":
                System.out.println("TTTTTTTTTTTTTTT receive TERMINATE message from " + msg.getSender());
                if (this.children.size() != 0) broadcastChildren("TERMINATE");
                this.terminate = true;
                Thread.sleep(1000);
                break;
            default:
                System.out.println("#$%%^&#$%@%#$%^&$^&#$%@#%@%receive a " + msg.getType() + " message from " + msg.getSender());
                break;
        }
    }

    @Override
    public void run() {
        try {
            listen();
            if (this.nodeID == 0) makeSpanningTree();
            sendAppMsg();
            chandyLamport();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws UnknownHostException {
        Node node = new Node();
        Thread node_thread = new Thread(node);
        System.out.println("node thread started ...");
        node_thread.start();
//        HashMap<Integer, String> a = new HashMap<>();
//        a.put(1, "a");
//        a.put(2, "b");
//        a.put(3, "c");
//        HashMap<Integer, String> b = new HashMap<>();
//        b.put(4, "d");
//        b.put(5, "e");
//        b.put(3, "g");
//        a.putAll(b);
//        for (HashMap.Entry<Integer, String> entry : a.entrySet()) {
//            System.out.print("key: " + entry.getKey() + " value: " + entry.getValue());
//            System.out.print("\n");
//        }

    }

}
