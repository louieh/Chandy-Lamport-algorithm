
import java.lang.Thread;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class ChandyLamport extends Thread {

    Node node;

    public ChandyLamport(Node node) {
        this.node = node;
    }


    @Override
    public void run() {
        while (true) {
            if (this.node.nodeID == 0) {
                if (!this.node.CLStarted) {
                    // record local state and send mark
                    this.node.statusBuffer = this.node.status;
                    this.node.timestampBuffer = this.node.timestamp_array;
                    this.node.broadcast("MARKER");
                    this.node.CLStarted = true;
                } else { // CL protocl have already started
                    if (this.node.statusCollection.size() == this.node.outgoingNodeList.size() && !this.node.ifMAPStop) {
                        // print statusCollection and print timestampCollection to make sure they indeed concurrent
                        // clear them
                        this.node.statusCollection.clear();
                        this.node.timestampCollection.clear();
                        this.node.statusBuffer = this.node.status;
                        this.node.timestampBuffer = this.node.timestamp_array;
                        this.node.broadcast("MARKER");
                    }
                }
            }
        }
    }
}