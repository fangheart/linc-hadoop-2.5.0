package org.apache.hadoop.yarn.server.major;

/**
 * Created by Majorshi on 16/12/11.
 */
public class MajorAppInfo {
    public String AMhostname;
    public String AppattemptID;
    public int numMaps;
    public int numtotalContainer;
    public int numCompletedContainers;
    public int numAllocatedContainers;
    public int numFailedContainers;
    public int numRequestedContainers;
    MajorAppInfo(String AMhostname, String AppattemptID, int numMaps, int numtotalContainer, int numCompletedContainers, int numAllocatedContainers, int numFailedContainers, int numRequestedContainer) {
        this.AMhostname = AMhostname;
        this.AppattemptID = AppattemptID;
        this.numMaps = numMaps;
        this.numtotalContainer = numtotalContainer;
        this.numCompletedContainers = numCompletedContainers;
        this.numAllocatedContainers = numAllocatedContainers;
        this.numFailedContainers = numFailedContainers;
        this.numRequestedContainers = numRequestedContainer;
    }
}
