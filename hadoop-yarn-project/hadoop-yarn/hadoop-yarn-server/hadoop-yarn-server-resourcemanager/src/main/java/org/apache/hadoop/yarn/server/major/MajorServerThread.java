package org.apache.hadoop.yarn.server.major;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
/**
 * Created by Majorshi on 16/12/10.
 */
public class MajorServerThread extends Thread {
    private String name;
    public MajorServerThread(String name) {

        this.name=name;
    }
    public void run() {
        try {
            // 设置服务端口为 7911
            TServerSocket serverTransport = new TServerSocket(7911);
            // 设置协议工厂为 TBinaryProtocol.Factory
            Factory proFactory = new TBinaryProtocol.Factory();
            // 关联处理器与 Server.Hello 服务的实现
            TProcessor processor = new Major.Processor(new MajorImpl());
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).protocolFactory(proFactory).processor(processor));
            System.out.println("Start server on port 7911...");
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }
}
