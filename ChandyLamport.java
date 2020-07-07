
import java.lang.Thread;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class ChandyLamport implements Runnable {

    Node node;

    public ChandyLamport(Node node) {
        this.node = node;
    }


    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (this.node.nodeID == 0) {
                System.out.println(this.node.test);
                if (!this.node.CLStarted && !this.node.ifMAPStop) {
                    this.node.statusBuffer = this.node.status;
                    this.node.timestampBuffer = this.node.timestamp_array;
                    System.out.println("@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@@#@#@send MARKER #$#$#$#$#$#$#$#$");
                    this.node.broadcast("MARKER");
                    System.out.println("^&*&*&*&*&*&*&*&*&*&*&*&* set CLStarted to true chandylamport function");
                    this.node.CLStarted = true;
                }
            }
        }
    }
}