package org.apache.hadoop.yarn.server.major;

/**
 * Created by Majorshi on 17/1/5.
 */
public class MajorNodeInfo {
    public enum MajorNodeState {
        LOST, ALIVE
    }
    public String nodeName;
    public MajorNodeState state;
    public String location;
    public double bandwithUsed;
    public int memory;
    public long lastReportTime;
    public MajorNodeInfo(String name) {
        this.nodeName = name;
        this.state = MajorNodeState.ALIVE;
        this.bandwithUsed = 0;
        this.lastReportTime = System.currentTimeMillis();
    }

}
