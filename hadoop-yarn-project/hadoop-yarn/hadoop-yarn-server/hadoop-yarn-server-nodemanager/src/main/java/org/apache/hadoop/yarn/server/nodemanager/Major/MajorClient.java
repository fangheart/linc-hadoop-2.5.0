package org.apache.hadoop.yarn.server.nodemanager.Major;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorClient {
    public static void updateInfo(String nodeid) {
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            // 调用服务的 helloVoid 方法
//            client.updateNodeInfo(nodeid, NetUsage.getInstance().getUsage());
            transport.close();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
