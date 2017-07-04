package org.apache.hadoop.yarn.server.major;
import org.apache.thrift.TException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.List;
import java.util.Map;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorImpl implements Major.Iface{
    private static final Log LOG = LogFactory.getLog("Major");
    @Override
    public boolean addApplication(int mapnum, String appid) throws org.apache.thrift.TException {
        MajorServer.getInstance().addApplication(appid, mapnum);
        return true;
    }

    public int assignContainerOnNodeManager(String nmid) throws org.apache.thrift.TException {
        return 1;
    }

    public int assignContainerForAMOnNodeManager(String nmid, String appid) throws org.apache.thrift.TException {
        return 1;
    }

    public String chooseNodeToGetBlock(List<String> nodeids, String blockid, String hostname, String src, long offset, long length) throws org.apache.thrift.TException {
        MajorServer.getInstance().updateBlockChoices(nodeids, blockid);
        MajorServer.getInstance().logChooseDataNode(nodeids, blockid, hostname, src, offset, length);
        return MajorServer.getInstance().chooseDataNode(nodeids, blockid, hostname, src, offset, length);
    }

    public boolean updateNodeInfo(String nodeid, double bandwithused) throws org.apache.thrift.TException {
        MajorServer.getInstance().updateNodeInfo(nodeid, bandwithused);
        return true;
    }

    public boolean updateTaskInfo(String appid, Map<String,List<String>> taskinfo, int totalContainers, int core, int memory) throws org.apache.thrift.TException {
        if (totalContainers == 0) return true;
        LOG.info("||||| 应用 " + appid + " 发来Task Info, 共需求 " + totalContainers + " 个Container");
        MajorServer.getInstance().updateTaskInfo(appid, taskinfo, totalContainers, core, memory);
        return true;
    }

    public Map<String,String> getTaskAllocatedInfo(String appid, List<String> taskids) throws org.apache.thrift.TException {
        return MajorServer.getInstance().getTaskContainerMap(appid, taskids);
    }

    public boolean updateTaskBlockMap(String appid, Map<String,String> taskblockmap) throws org.apache.thrift.TException {
        return true;
    }

    public boolean justLog(String log) throws org.apache.thrift.TException {
        LOG.info("justLog: " + log);
        if (log.startsWith("DEALLOCATE_CONTAINER")) {
            String[] strs = log.split("\\|");
            String appid = strs[1];
            String tid = strs[2];
            String host = strs[3];
            MajorServer.getInstance().updateContainerRunningTime(appid, tid, host);
        }
        return true;
    }

    public boolean blockLoadCompleted(String blokid, String nodeid, String backup) throws org.apache.thrift.TException {
        MajorServer.getInstance().blockLoadCompleted(blokid, nodeid, backup);
        return true;
    }

    public boolean updateSplits(String appid, Map<String,String> src, Map<String,List<String>> locations, Map<String,Long> offset, Map<String,Long> length) throws org.apache.thrift.TException {
        MajorServer.getInstance().updateSplits(appid, src, locations, offset, length);
        return true;
    }
}
