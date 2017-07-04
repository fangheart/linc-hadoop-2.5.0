package org.apache.hadoop.yarn.server.nodemanager.Major;

import org.apache.log4j.Logger;

import java.io.*;
import java.text.DecimalFormat;

/**
 * 采集网络带宽使用率
 */
public class NetUsage extends Thread{

    private static Logger log = Logger.getLogger(NetUsage.class);
    private static NetUsage INSTANCE = null;
    private final static float TotalBandwidth = 50;   //网口带宽,Mbps
    private String NodeId;
    private float usage;

    private NetUsage(){

    }

    public static NetUsage getInstance(){
        if (INSTANCE == null) {
            INSTANCE = new NetUsage();
            INSTANCE.usage = 0.0f;
//            INSTANCE.start();
            INSTANCE.NodeId = "";
        }
        return INSTANCE;
    }

    public float getUsage() {
        return INSTANCE.usage;
    }

    public String getNodeId() {
        return NodeId;
    }

    public void setNodeId(String nodeId) {
        NodeId = nodeId;
    }

    /**
     * @Purpose:采集网络带宽使用率
     * @param args
     * @return float,网络带宽使用率,小于1
     */
    public void run() {
        while(true) {
            log.info("开始收集网络带宽使用率");
            float netUsage = 0.0f;
            Process pro1, pro2;
            Runtime r = Runtime.getRuntime();
            try {
                String command = "cat /proc/net/dev";
                //第一次采集流量数据
                long startTime = System.currentTimeMillis();
                pro1 = r.exec(command);
                BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
                String line = null;
                long inSize1 = 0, outSize1 = 0;
                while ((line = in1.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("eth")) {
//                    log.info(line);
                        String[] temp = line.split("\\s+");
//                    for (String str : temp) {
//                        log.info(str + "|");
//                    }
                        inSize1 += Long.parseLong(temp[1]); //Receive bytes,单位为Byte
                        outSize1 += Long.parseLong(temp[9]);             //Transmit bytes,单位为Byte
//                        break;
                    }
                }
                in1.close();
                pro1.destroy();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    log.info("NetUsage休眠时发生InterruptedException. " + e.getMessage());
                    log.info(sw.toString());
                }
                //第二次采集流量数据
                long endTime = System.currentTimeMillis();
                pro2 = r.exec(command);
                BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
                long inSize2 = 0, outSize2 = 0;
                while ((line = in2.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("eth")) {
//                    log.info(line);
                        String[] temp = line.split("\\s+");
                        inSize2 += Long.parseLong(temp[1]);
                        outSize2 += Long.parseLong(temp[9]);
//                        break;
                    }
                }
                if (inSize1 != 0 && outSize1 != 0 && inSize2 != 0 && outSize2 != 0) {
                    float interval = (float) (endTime - startTime) / 1000;
                    //网口传输速度,单位为bps
                    float curRate = (float) (inSize2 - inSize1 + outSize2 - outSize1) * 8 / (1000000 * interval);
                    netUsage = curRate / TotalBandwidth * 100;
                    INSTANCE.usage = netUsage;
                    float insize = inSize2 - inSize1;
                    log.info("InSize: " + insize + "bytes");
                    float outsize = outSize2 - outSize1;
                    log.info("OutSize: " + outsize + "bytes");
                    log.info("本节点网口速度为: " + curRate + " Mbps");
                    DecimalFormat df = new DecimalFormat("######0.000000");
                    log.info("本节点网络带宽使用率为: " + df.format(netUsage) + "%");
                }
                in2.close();
                pro2.destroy();
                //send NodeInfo To MAJOR
                DecimalFormat df = new DecimalFormat("######0.000000");
                if (INSTANCE.NodeId.length() != 0) {
//                    log.info("======send NodeInfo To MAJOR:" + INSTANCE.NodeId + " " + df.format(NetUsage.getInstance().getUsage()));
//                    MajorClient.updateInfo(INSTANCE.NodeId);
                }
            } catch (IOException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                log.error("NetUsage发生InstantiationException. " + e.getMessage());
                log.error(sw.toString());
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

    }
}