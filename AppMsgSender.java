import java.io.*;
import java.lang.Thread;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Random;

public class AppMsgSender extends Thread {
    // ArrayList<Socket> clientSockerList = new ArrayList<>();
    Node node;
    ArrayList<Integer> outgoingNodeList;

    public AppMsgSender(Node node, ArrayList<Integer> outgoingNodeList) throws IOException, InterruptedException {
        this.node = node;
        this.outgoingNodeList = outgoingNodeList;
    }

    public void sendApMsg() {
        if (this.node.nodeID == 0 && this.node.index0StartWait) {
            try {
                sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.node.index0StartWait = false;
        }
        if (this.node.status.equals("active")) {
//            if (this.node.appMsgSent >= this.node.maxNumber) {
//                System.out.println("the number of app message of the node: " + this.node.nodeID + " sent has already larger than maxNumber but don't know why it become active again maybe something wrong....");
//                this.node.status = "passive";
//                for (Socket socket : clientSockerList) {
//                    try {
//                        socket.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                System.out.println("the node: " + this.node.nodeID + " over.");
//                return;
//            }

            // send a message
            Random rand = new Random();
            int randSocketIndex = rand.nextInt(this.outgoingNodeList.size());
            int randNodeID = this.outgoingNodeList.get(randSocketIndex);
            String hostname = this.node.NodeInfoList.get(randNodeID).get("hostname");
            int port = Integer.parseInt(node.NodeInfoList.get(randNodeID).get("port"));

            this.node.timestamp_array[this.node.nodeID] += 1; // update timestamp: sending a message C[i] += 1 and piggyback C on message
            Message msg = new Message.MessageBuilder()
                    .from(this.node.nodeID)
                    .to(this.node.outgoingNodeList.get(randSocketIndex))
                    .type("application")
                    .timestamp_array(this.node.timestamp_array)
                    .build();
            msg.sendMsg(msg, hostname + ".utdallas.edu", port);

            this.node.appMsgSent++;
            System.out.println("I'm " + this.node.nodeID + "I have sent " + this.node.appMsgSent + "message and my status now is " + this.node.status);

            if (!this.node.passived) {
                if (this.node.appMsgSent == this.node.perActive) {
                    this.node.status = "passive";
                    this.node.passived = true;
                }
            } else {
                if (this.node.appMsgSent == this.node.maxNumber) {
                    this.node.status = "passive";
//                    for (Socket socket : clientSockerList) {
//                        try {
//                            socket.close();
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    }
                    System.out.println("########### the node: " + this.node.nodeID + " over.....sent message: " + this.node.appMsgSent + " status now: " + this.node.status);
                    System.out.println("My timestamp now is: ");
                    for (int timestamp : this.node.timestamp_array) {
                        System.out.print(timestamp + ", ");
                    }
                    return;
                }
            }
            try {
                // System.out.println("--- wait to send next message...");
                sleep(this.node.minSendDelay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("this is Node: " + this.node.nodeID + " at " + this.node.hostName + " status: " + this.node.status + " sent message: " + this.node.appMsgSent);
            try {
                sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            sendApMsg();
        }
    }
}
