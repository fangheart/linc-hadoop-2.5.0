package org.apache.hadoop.mapreduce.v2.app.Major;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.Map;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorClient {
    public static void updateTaskInfo(String appid, Map<String,List<String>> taskinfo, int totalContainers, int core, int memory) {
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            // 调用服务的 helloVoid 方法
            client.updateTaskInfo(appid, taskinfo, totalContainers, core, memory);
            transport.close();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public static Map<String,String> getTaskContainerInfo(String appid, List<String> containerids) {
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            // 调用服务的 helloVoid 方法
            Map<String,String> re = client.getTaskAllocatedInfo(appid, containerids);
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

    public static boolean justLog(String log) {
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            // 调用服务的 helloVoid 方法
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
}
