import java.lang.Thread;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class NodeListener extends Thread {
    Node node;
    int port;
    private ServerSocket serverSocket;

    public NodeListener(Node node, int port) throws IOException {
        this.node = node;
        this.port = port;
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket server = serverSocket.accept();
                ObjectInputStream in = new ObjectInputStream(server.getInputStream());
                Message msg = (Message) in.readObject();
                this.node.receiveMsg(msg);
            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
            } catch (IOException e) {
                e.printStackTrace();
                break;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (this.node.ifMAPStop || this.node.terminate) {
                System.out.println("++++++++++++++ Node " + this.node.nodeID + " listener thread stop...");
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
        }
    }
}