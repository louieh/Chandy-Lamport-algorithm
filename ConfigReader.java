import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class ConfigReader {
    int nodeNum;
    int myNodeIndex;
    String myHostName;
    int perActive;
    int minSendDelay;
    int snapShotDelay;
    int maxNumber;
    ArrayList<Map<String, String>> NodeInfoList;
    ArrayList<Integer> outgoingNodeList;

    public ConfigReader() throws UnknownHostException {
        read();
        printConfigInfo(this);
    }

    private void read() throws UnknownHostException {
        InetAddress localHostInfo = InetAddress.getLocalHost();
        this.myHostName = localHostInfo.getHostName().split("\\.")[0];
        boolean firstline = false;
        int parsedNode = 0;
        int parsedOutgoingIndex = 0;
        this.NodeInfoList = new ArrayList<Map<String, String>>();
        this.outgoingNodeList = new ArrayList<>();
        System.out.println("current path: " + System.getProperty("user.dir"));
        try (BufferedReader br = new BufferedReader(new FileReader("./config.txt"))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] line_list = line.split("\\s+");
                if (line_list.length == 1 || line_list[0].equals("#")) continue;
                if (!firstline) {
                    this.nodeNum = Integer.parseInt(line_list[0]);
                    int minPerActive = Integer.parseInt(line_list[1]);
                    int maxPerActive = Integer.parseInt(line_list[2]);
                    this.perActive = ThreadLocalRandom.current().nextInt(minPerActive, maxPerActive + 1);
                    this.minSendDelay = Integer.parseInt(line_list[3]);
                    this.snapShotDelay = Integer.parseInt(line_list[4]);
                    this.maxNumber = Integer.parseInt(line_list[5]);
                    firstline = true;
                } else {
                    if (parsedNode != this.nodeNum) {
                        if (line_list[1].equals(this.myHostName)) this.myNodeIndex = parsedNode;
                        Map<String, String> temp = new HashMap<>();
                        temp.put("hostname", line_list[1]);
                        temp.put("port", line_list[2]);
                        this.NodeInfoList.add(temp);
                        parsedNode++;
                    } else {
                        if (parsedOutgoingIndex == this.myNodeIndex) {
                            for (String x : line_list) {
                                if (x.equals("#")) break;
                                this.outgoingNodeList.add(Integer.parseInt(x));
                            }
                            break;
                        }
                        parsedOutgoingIndex++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getPort() {
        return Integer.parseInt(this.NodeInfoList.get(this.myNodeIndex).get("port"));
    }

    private static void printConfigInfo(ConfigReader a) {
        System.out.println("nodeNum: " + a.nodeNum);
        System.out.println("myNodeIndex: " + a.myNodeIndex);
        System.out.println("myHostName: " + a.myHostName);
        System.out.println("PerActive: " + a.perActive);
        System.out.println("minSendDelay: " + a.minSendDelay);
        System.out.println("snapShotDelay: " + a.snapShotDelay);
        System.out.println("maxNumber: " + a.maxNumber);
        ArrayList<Map<String, String>> NodeInfoList = a.NodeInfoList;
//        for (int i = 0; i < NodeInfoList.size(); i++) {
//            System.out.println("node index: " + i + "node host name: " + NodeInfoList.get(i).get("hostname") + "node port" + NodeInfoList.get(i).get("port"));
//        }
        ArrayList<Integer> outGoing = a.outgoingNodeList;
        for (Integer temp : outGoing) System.out.println(temp);
    }

    public static void main(String[] args) throws UnknownHostException {
        System.out.println("this is main function of Config Reader class");
        ConfigReader a = new ConfigReader();
        printConfigInfo(a);
    }
}