import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Ner4perClient implements Serializable {
    public static final String SERVER_IP = "10.20.7.35";
    public static final int SERVER_PORT = 6230;//端口号 新部服务端口.
    public static final int TIMEOUT = 2000000;


    private void startClient() {
        List<String> serverList = new ArrayList<String>();
//		serverList.add("10.20.7.47");
        serverList.add(SERVER_IP);
        for (String serverIP : serverList) {
            System.out.println("procesing serverIP >>>" + serverIP);
            TTransport transport = null;
            try {
                transport = new TSocket(serverIP, SERVER_PORT, TIMEOUT);
                TProtocol protocol = new TBinaryProtocol(transport);

                Ner4Per.Client client = new Ner4Per.Client(protocol);
                transport.open();

                String title = "";
                String content = "牡丹江通报2起扶贫领域违纪违法问题典型案例为深入贯彻落实中央和省市纪委全会部署，决定给予刘东存同志党内严重警告处分，违纪所得予以收缴。";

                double weight = 0.0;
//				for (int i = 0; i < 1; i++) {
                long startTime = System.currentTimeMillis();
                String pers = "";
                pers = client.getpers(title, content, weight);
                System.out.println("pers = " + pers + "\t" + (System.currentTimeMillis() - startTime) + "ms. serverIP=" + serverIP);
//				}
            } catch (TTransportException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null != transport) {
                    transport.close();
                }
            }
        }
    }

    public static String startClient(String host, int port, int timeout,String title ,String content) {
        String pers = "";
        TTransport transport = null;
        try {
            transport = new TSocket(host, port, timeout);
            TProtocol protocol = new TBinaryProtocol(transport);

            Ner4Per.Client client = new Ner4Per.Client(protocol);
            transport.open();

            double weight = 0.0;
            long startTime = System.currentTimeMillis();
            pers = client.getpers(title, content, weight);

            System.out.println("pers = " + pers + "\t" + (System.currentTimeMillis() - startTime) + "ms");
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return pers;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        Ner4perClient client = new Ner4perClient();
//        client.startClient();
        client.startClient("10.20.7.39",62300,10*1000,"lkadkfa曾国藩","lkadkfa曾国藩");


    }

}
