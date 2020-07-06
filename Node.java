import java.io.*;
import java.lang.Thread;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Map;
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
    public ArrayList<Map<String, String>> NodeInfoList;
    public ArrayList<Integer> outgoingNodeList;
    public String status; // passive or acitve
    public int appMsgSent; // application message sent
    public boolean passived; // mark the node has already became passive
    public boolean index0StartWait;
    public int parentID = -1; // parent id in the spanning tree
    public ArrayList<Integer> children = new ArrayList<>(); // children in the spanning tree

    public Node() throws UnknownHostException {
        ConfigReader config = new ConfigReader();
        this.nodeNum = config.nodeNum;
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
    }

    public void makeSpanningTree() throws IOException, InterruptedException {
        for (int outgoingNode : this.outgoingNodeList) {
            String hostname = this.NodeInfoList.get(outgoingNode).get("hostname");
            int port = Integer.parseInt(this.NodeInfoList.get(outgoingNode).get("port"));
            Message msg = new Message.MessageBuilder()
                    .from(this.nodeID)
                    .to(outgoingNode)
                    .type("search")
                    .build();
            msg.sendMsg(msg, hostname + ".utdallas.edu", port);
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

    public void receiveMsg(Message msg) throws InterruptedException {
        if (msg.getType().equals("application")) {
            System.out.println(">>>>>>>>>>>>>>receive an app message from " + msg.getSender() + " status now: " + this.status);
            if (this.appMsgSent < this.maxNumber && !this.status.equals("active")) {
                System.out.println("appMsgSent less than maxNumber, change status to active");
                this.status = "active";
            }
        } else if (msg.getType().equals("accept")) {
            System.out.println("******* receive accept message from " + msg.getSender() + "add it in children list. My " + this.nodeID + "children now are: ");
            this.children.add(msg.getSender());
            for (int child : this.children) System.out.print(child + ", ");
        } else if (msg.getType().equals("search")) {
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
                for (int outgoingNode : this.outgoingNodeList) {
                    if (outgoingNode != 0) {
                        String broadcast_hostname = this.NodeInfoList.get(outgoingNode).get("hostname");
                        int broadcast_port = Integer.parseInt(this.NodeInfoList.get(outgoingNode).get("port"));
                        Message searchMsg = new Message.MessageBuilder()
                                .from(this.nodeID)
                                .to(outgoingNode)
                                .type("search")
                                .build();
                        searchMsg.sendMsg(searchMsg, broadcast_hostname + ".utdallas.edu", broadcast_port);
                        Thread.sleep(1000);
                    }
                }
            }
        } else {
            System.out.println(">>>>>>>>>>>>>>>receive a " + msg.getType() + " message from " + msg.getSender());
        }
    }

    @Override
    public void run() {
        try {
            listen();
            sendAppMsg();
            if (this.nodeID == 0) makeSpanningTree();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        Node node = new Node();
        Thread node_thread = new Thread(node);
        System.out.println("node thread started ...");
        node_thread.start();
    }

}
