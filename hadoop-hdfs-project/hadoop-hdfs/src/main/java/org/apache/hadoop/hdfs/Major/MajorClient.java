package org.apache.hadoop.hdfs.Major;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorClient {
    public static String chooseNodeToGetBlock(List<String> nodeids, String blockid, String hostname, String src, long offset, long length) throws org.apache.thrift.TException {
    try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            String re = client.chooseNodeToGetBlock(nodeids, blockid, hostname, src, offset, length);
            transport.close();
            return re;
        } catch (TTransportException e) {
            e.printStackTrace();
            return null;
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static boolean justLog(String log)  throws org.apache.thrift.TException {
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            boolean re = client.justLog(log);
            transport.close();
            return re;
        } catch (TTransportException e) {
            e.printStackTrace();
            return false;
        } catch (TException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean blockLoadCompleted(String blkid, String nodeid, String backup)  throws org.apache.thrift.TException {
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            boolean re = client.blockLoadCompleted(blkid, nodeid, backup);
            transport.close();
            return re;
        } catch (TTransportException e) {
            e.printStackTrace();
            return false;
        } catch (TException e) {
            e.printStackTrace();
            return false;
        }
    }
}
