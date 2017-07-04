package org.apache.hadoop.yarn.server.major;

/**
 * Created by Majorshi on 16/12/18.
 */
public class MajorContainerPlan {
    public int assignedContainerNum;
    public int containerNumToAssign;
    MajorContainerPlan(int as, int toas) {
        this.assignedContainerNum = as;
        this.containerNumToAssign = toas;
    }
}
