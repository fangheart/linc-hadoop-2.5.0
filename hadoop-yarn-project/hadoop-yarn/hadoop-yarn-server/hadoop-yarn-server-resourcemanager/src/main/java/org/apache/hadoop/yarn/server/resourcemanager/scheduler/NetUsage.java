package org.apache.hadoop.yarn.server.resourcemanager.scheduler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;

import org.apache.log4j.Logger;

/**
 * 采集网络带宽使用率
 */
public class NetUsage extends Thread{

    private static Logger log = Logger.getLogger(NetUsage.class);
    private static NetUsage INSTANCE = new NetUsage();
    private final static float TotalBandwidth = 1000;   //网口带宽,Mbps
    private float usage;

    private NetUsage(){

    }

    public static NetUsage getInstance(){
        return INSTANCE;
    }

    public float getUsage() {
        return INSTANCE.usage;
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
                    if (line.startsWith("eth0")) {
//                    log.info(line);
                        String[] temp = line.split("\\s+");
//                    for (String str : temp) {
//                        log.info(str + "|");
//                    }
                        inSize1 = Long.parseLong(temp[1]); //Receive bytes,单位为Byte
                        outSize1 = Long.parseLong(temp[9]);             //Transmit bytes,单位为Byte
                        break;
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
                    if (line.startsWith("eth0")) {
//                    log.info(line);
                        String[] temp = line.split("\\s+");
                        inSize2 = Long.parseLong(temp[1]);
                        outSize2 = Long.parseLong(temp[9]);
                        break;
                    }
                }
                if (inSize1 != 0 && outSize1 != 0 && inSize2 != 0 && outSize2 != 0) {
                    float interval = (float) (endTime - startTime) / 1000;
                    //网口传输速度,单位为bps
                    float curRate = (float) (inSize2 - inSize1 + outSize2 - outSize1) * 8 / (1000000 * interval);
                    netUsage = curRate / TotalBandwidth;
                    float insize = inSize2 - inSize1;
                    log.info("InSize: " + insize + "bytes");
                    float outsize = outSize2 - outSize1;
                    log.info("OutSize: " + outsize + "bytes");
                    log.info("本节点网口速度为: " + curRate + " Mbps");
                    DecimalFormat df = new DecimalFormat("######0.00");
                    log.info("本节点网络带宽使用率为: " + df.format(netUsage * 100) + "%");
                }
                in2.close();
                pro2.destroy();
            } catch (IOException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                log.error("NetUsage发生InstantiationException. " + e.getMessage());
                log.error(sw.toString());
            }
            INSTANCE.usage = netUsage;
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