package org.apache.hadoop.yarn.server.major;
import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.tools.javac.code.Attribute;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorServer {
    /*
    resource类，包含cpu个数，内存大小
     */
    public class Resource {
        public Integer core;
        public Integer memory;
        public Resource (Integer c, Integer m) {
            core = c;
            memory = m;
        }
        public Resource (Resource r) {
            core = r.core;
            memory = r.memory;
        }
    }


    //定义任务状态的枚举
    public enum TaskStatus {
        UNASSIGNED, ASSIGNED, ALLOCATED, ERROR, COMPLETED
    }


    /*
    定义任务类 包含资源请求量，nodeId,appId,Location以及task状态，当前等待的队列ID
     */
    public class Task implements Comparable  {
        public String no;
        public Resource request;
        public String nodeId;
        public String appid;
        public HashSet<String> locations;
        public TaskStatus status;
        public String nowQueueId;   //当前等待的队列ID（NodeID）
        public boolean isDataLocal;
        public HashSet<String> pre_locations;

        public Task (String n, Resource r, HashSet<String> l, String aid) {
            no = n;
            request = r;
            locations = l;
            status = TaskStatus.UNASSIGNED;
            appid = aid;
            isDataLocal = false;
            pre_locations = null;
        }
        public Task (Task a) {
            no = a.no;
            request = a.request;
            nodeId = a.nodeId;
            status = a.status;
            appid = a.appid;
            locations = a.locations;
            nowQueueId = a.nowQueueId;
            isDataLocal = a.isDataLocal;
            pre_locations = a.pre_locations;
        }

        public int compareTo(Object o) {
            Task a= (Task)o;
            return this.no.compareTo(a.no);
        }
    }

    /*
    记录block要从哪传递到哪
     */
    public class Transport {
        public String id;       //内部id="hostname#location1|location2|location3@srchostname"
        public String blockid;
        public String targetNode;
        public String srcNode;
        public Transport(String bid, String blkid, String target, String src) {
            id = bid;
            blockid = blkid;
            targetNode = target;
            srcNode = src;
        }
    }


    /*
    定义节点类，包含nodeid，resource总量，现在拥有的resource，已分配的task列表，
     */

    public class Node {
        public String nodeId;
        public Resource totalResource;
        public Resource nowResource;
        public ArrayList<String> allocatedTasks;
        public Integer greedy1_ContainerNum;
        public HashSet<String> taskLocated;
        public Double bandwithUsed;
        public long lastUpdateTime;
        public HashMap<String, HashSet<String>> transportTarget;        //计划传输<String-"location1|location2|location3...", HashSet<String-目标节点>>
        public HashMap<String, Transport> transportingBlock;   //正在传输<blockid, Transport>
        public int totalTransports;     //总传输量
        public Node (String id, Resource r) {
            nodeId = id;
            totalResource = new Resource(r.core, r.memory);
            nowResource = new Resource(r.core, r.memory);
            allocatedTasks = new ArrayList<String>();
            taskLocated = new HashSet<String>();
            greedy1_ContainerNum = 0;
            totalTransports = 0;
            transportTarget = new HashMap<String, HashSet<String>>();
            transportingBlock = new HashMap<String, Transport>();
        }
        public Node (Node n) {
            nodeId = n.nodeId;
            totalResource = new Resource(n.totalResource);
            nowResource = new Resource(n.nowResource);
            allocatedTasks = new ArrayList<String>(n.allocatedTasks);
            taskLocated = new HashSet<String>();
            greedy1_ContainerNum = n.greedy1_ContainerNum;
            totalTransports = n.totalTransports;
            transportTarget = new HashMap<String, HashSet<String>>(n.transportTarget);
            transportingBlock = new HashMap<String, Transport>(n.transportingBlock);
        }

        public boolean canAllocateTask(Task t) {
            if (nowResource.core >= t.request.core && nowResource.memory >= t.request.memory) {
                return true;
            }
            return false;
        }

        public void allocateTask(Task t) {
            if (canAllocateTask(t)) {
                allocatedTasks.add(t.no);
                nowResource.core = nowResource.core - t.request.core;
                nowResource.memory = nowResource.memory - t.request.memory;
                greedy1_ContainerNum--;
            }
        }

        public void unallocateTask(Task t) {
            if (allocatedTasks.contains(t.no)) {
                allocatedTasks.remove(t.no);
                nowResource.core = nowResource.core + t.request.core;
                nowResource.memory = nowResource.memory + t.request.memory;
            }
        }
    }


    /*
    定义split类，包含id,源位置block offset，长度，task的index,location等
     */
    public class Split {
        public String id;
        public String src;
        public long offset;
        public long length;
        public Integer taskIndex;
        public HashSet<String> location;
        public Split(String i, String s, long o, long l, HashSet<String> loc) {
            id = i;
            src = s;
            offset = o;
            length = l;
            location = loc;
            taskIndex = -1;
        }
    }


    /*
    需求的split链
     */
    public class SplitChain {
        public Split fileStartSplit;
        public Integer targetIndex;
        public Split targetSplit;
        public Split chainSplit;
        public HashSet<String> locations;
        public boolean isFound;
        public SplitChain(Split f, Split t, Split c, HashSet<String> l, int index) {
            fileStartSplit = f;
            targetSplit = t;
            chainSplit = c;
            locations = l;
            targetIndex = index;
            isFound = false;
        }
    }



    //NEW
    private HashMap<String, Node> nodeMap;
    private HashMap<String, HashMap<String, Task>> applicationTaskMap;
    private HashMap<String, ArrayList<Task>> nodeTaskQueue;
    private HashMap<String, HashSet<Task>> applicationUnassignedTaskPool;
    private HashMap<String, HashMap<String, Split>> applicationSplitMap;
    private HashMap<String, HashSet<String>> applicationSplitPool;//用于区分inputfile和中间文件
    private HashMap<String, Long> applicationStartTime;
    private HashMap<String, Integer> applicationUnlocalityNumber;  //记录每个application没有实现locality的task数量
    private HashMap<String, Thread> applicationAllocater;
    private HashMap<String, Integer> applicationAMGetNumber;
    private HashMap<String, Integer> applicationChooseDNNumber;
    private HashMap<String, HashSet<Task>> applicationAssignedTaskPool;
    private HashMap<String, Boolean> applicationStartDefaultAllocate;   //开关，开启后使用默认的调度方式(弃用)
    private HashMap<String, Integer> applicationIdleTime;   //记录当Node心跳时，如果有空余资源但没有Task可分配的次数
    private HashMap<String, HashSet<SplitChain>> applicationSplitChain;
    private HashMap<String, HashMap<String, Long>> applicationTaskStartTime;
    private HashMap<String, HashMap<String, Long>> applicationTaskRunningTime;
    private HashMap<String, HashMap<String, String>> applicationTaskFinalNode;
    //NEW END
    private static MajorServer instance;
    private boolean isOpen;
    private MajorServerThread thread;
    private HashMap<String, MajorNodeInfo> nodeInfo;
    private HashMap<String, MajorAppInfo> appInfo;
    private HashMap<String, Integer> appMapNumer;
    private HashMap<String, Integer> nodeAvaliableMemory;
    private HashMap<String, List<String>> blockChoices;
    private HashMap<String, HashMap<String, Integer>> allocatedContainers;
    private HashMap<String, HashMap<String, ArrayList<String>>> applicationTaskHostMap;
    private HashMap<String, HashMap<String, ArrayList<String>>> applicationHostTaskMap;
    private HashMap<String, HashMap<String, String>> applicationContainerTaskMap;
    private HashMap<String, HashMap<String, MajorContainerPlan>> containerPlan;
    private static final Log LOG = LogFactory.getLog("Major");
    private static final Log StatLOG = LogFactory.getLog("Stat");
    private MajorServer (){

    }

    //MajorServer实例
    public static synchronized MajorServer getInstance() {
        if (instance == null) {
            instance = new MajorServer();
            instance.isOpen = false;
            instance.thread = null;
            instance.nodeInfo = new HashMap<String, MajorNodeInfo>();
            instance.appInfo = new HashMap<String, MajorAppInfo>();
            instance.blockChoices = new HashMap<String, List<String>>();
            instance.allocatedContainers = new HashMap<String, HashMap<String, Integer>>();
            instance.appMapNumer = new HashMap<String, Integer>();
            instance.containerPlan = new HashMap<String, HashMap<String, MajorContainerPlan>>();
            instance.nodeAvaliableMemory = new HashMap<String, Integer>();
            instance.applicationTaskHostMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
            instance.applicationHostTaskMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
            instance.applicationContainerTaskMap = new HashMap<String, HashMap<String, String>>();
            instance.applicationTaskStartTime = new HashMap<String, HashMap<String, Long>>();
            instance.applicationTaskRunningTime = new HashMap<String, HashMap<String, Long>>();
            instance.applicationTaskFinalNode = new HashMap<String, HashMap<String, String>>();
            //NEW
            instance.nodeMap = new HashMap<String, Node>();
            instance.applicationTaskMap = new HashMap<String, HashMap<String, Task>>();
            instance.nodeTaskQueue = new HashMap<String, ArrayList<Task>>();
            instance.applicationUnassignedTaskPool = new HashMap<String, HashSet<Task>>();
            instance.applicationSplitMap = new HashMap<String, HashMap<String, Split>>();
            instance.applicationSplitPool = new HashMap<String, HashSet<String>>();
            instance.applicationStartTime = new HashMap<String, Long>();
            instance.applicationAllocater = new HashMap<String, Thread>();
            instance.applicationUnlocalityNumber = new HashMap<String, Integer>();
            instance.applicationAMGetNumber = new HashMap<String, Integer>();
            instance.applicationChooseDNNumber = new HashMap<String, Integer>();
            instance.applicationAssignedTaskPool = new HashMap<String, HashSet<Task>>();
            instance.applicationStartDefaultAllocate = new HashMap<String, Boolean>();
            instance.applicationIdleTime = new HashMap<String, Integer>();
            instance.applicationSplitChain = new HashMap<String, HashSet<SplitChain>>();
        }
        return instance;
    }


    //初始化日志头打印
    public boolean run() {
        if (!instance.isOpen) {
            instance.thread = new MajorServerThread("MajorServer");
            LOG.info("======================================================");
            LOG.info("|                  MAJOR SERVER LOG                  |");
            LOG.info("======================================================");
            LOG.info("| Major Server 正在初始化");
            instance.thread.start();
            instance.isOpen = true;
            LOG.info("| Major Server 启动成功！");
            return instance.isOpen;
        } else {
            return true;
        }
    }

    //majorServer是否已经打开
    public boolean getisOpen() {
        return instance.isOpen;
    }

    //get方法区
    public HashMap<String, MajorNodeInfo> getNodeInfo() {
        return nodeInfo;
    }

    public HashMap<String, MajorAppInfo> getAppInfo() {
        return appInfo;
    }

    public HashMap<String, List<String>> getBlockChoices() {
        return blockChoices;
    }

    public HashMap<String, HashMap<String, Integer>> getAllocatedContainers() {
        return allocatedContainers;
    }


    public String formatAppId (String id) {
        if (id.startsWith("application")) {
            if (id.contains("_")) {
                String[] sps = id.split("_");
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < sps.length; i++) {
                    if (i == 0) continue;
                    if (sb.length() != 0) {
                        sb.append("_");
                    }
                    sb.append(sps[i]);
                }
                return sb.toString();
            }
            else
                return id;
        } else {
            return id;
        }
    }

    public String formatSplitSrc (String src) {
        if (src.indexOf("/user") != -1) {
            src = src.substring(src.indexOf("/user"));
        }
        return src;
    }

    public String formatNodeId (String id) {
        if (id.contains(":"))
            return id.split(":")[0];
        else
            return id;
    }

    public void updateNodeInfo(String nodeid, double bandwidth) {
        String nid = formatNodeId(nodeid);
        if (!nodeInfo.containsKey(nid)) {
            MajorNodeInfo ninfo = new MajorNodeInfo(nid);
            nodeInfo.put(nid, ninfo);
        }
        MajorNodeInfo nodeinfo = nodeInfo.get(nid);
        nodeinfo.bandwithUsed = bandwidth;
    }

    public void logChooseDataNode(List<String> nodeids, String blockid, String hostname, String src, long offset, long length) {
//        String s = formatSplitSrc(src) + "|" + offset + "|" + length;
//        LOG.info("!!!!!!logChooseDataNode: " + s);
//        for (HashSet<String> set : instance.applicationSplitPool.values()) {
//            if (set.contains(s)) {
//                LOG.info("!!!!!!找到 " + blockid + " 对应的Split:" + s);
//                break;
//            }
//        }
    }

    public void addApplication(String id, int num) {
        id = formatAppId(id);
        instance.appMapNumer.put(id, num);
        instance.containerPlan.put(id, new HashMap<String, MajorContainerPlan>());
        LOG.info("||||||||||||||||||||||||||||||||||||||发现新任务 " + id + " Mapper数量为 " + num + "||||||||||||||||||||||||||||||||||||||");
        instance.applicationStartTime.put(id, System.currentTimeMillis());
        instance.applicationAllocater.put(id, new Allocater(id));
        instance.applicationUnlocalityNumber.put(id, 0);
        instance.applicationAMGetNumber.put(id, 0);
        instance.applicationChooseDNNumber.put(id, 0);
        instance.applicationAssignedTaskPool.put(id, new HashSet<Task>());
        instance.applicationStartDefaultAllocate.put(id, false);
        instance.applicationIdleTime.put(id, 0);
        instance.applicationSplitChain.put(id, new HashSet<SplitChain>());
        instance.applicationTaskRunningTime.put(id, new HashMap<String, Long>());
        instance.applicationTaskStartTime.put(id, new HashMap<String, Long>());
        instance.applicationTaskFinalNode.put(id, new HashMap<String, String>());

    }

    public void updateBlockChoices(List<String> nodeids, String blockid) {
        instance.blockChoices.put(blockid + "|" + instance.blockChoices.size(), nodeids);
        StringBuilder sb = new StringBuilder();
        sb.append("| 收到来自 ");
        sb.append(blockid);
        sb.append(" 的Block读取请求, 可选节点为 ");
        for (String id: nodeids) {
            sb.append("|");
            sb.append(id);
        }
        LOG.info(sb.toString());
    }

    public void updateResourceRequest(List<ResourceRequest> ask, String appid) {

    }

    public boolean getApplicationStartDefualtAllocate(String appid) {
        appid = formatAppId(appid);
        return instance.applicationStartDefaultAllocate.get(appid);
    }

    //FIFO中调用，包含抢占
    public int getAllocatedContainerNumber(String nodeid, String appid, int requestCapability, int newMemory, int assignableContainers) {
        int avaliableContainers = newMemory / requestCapability;
        String nid = formatNodeId(nodeid);
        if (instance.getNodeInfo().containsKey(nid)) {
            int majorAvaliable = instance.getNodeInfo().get(nid).memory / requestCapability;
//            LOG.info("|| 记录的关于 " + nid + " 的资源数据:");
//            LOG.info("|| 记录的Memory:" + instance.getNodeInfo().get(nid).memory + "| " + newMemory);
//            LOG.info("|| 记录所得avaliableContainers:" + majorAvaliable + "| " + avaliableContainers);
        }
        LOG.info("!!! 节点 " + nodeid + " 开始计算申请Container数量,可用Container数量为 " + avaliableContainers);
        appid = formatAppId(appid);
        int result = 0;
        int appTaskLastIndex = 0;
        for (int i = 0; i < nodeTaskQueue.get(nid).size(); i++) {
            if (result >= avaliableContainers) break;
            Task t = nodeTaskQueue.get(nid).get(i);
            if (t.appid.equals(appid)) {
                appTaskLastIndex++;
                if (t.status == TaskStatus.ASSIGNED) {
                    result++;
                    t.status = TaskStatus.ALLOCATED;
                    instance.applicationAssignedTaskPool.get(appid).remove(t);
                }
            }
        }
        LOG.info("!!! 节点 " + nodeid + " 队列中找到Task数量为 " + result);
//        if (result < avaliableContainers) {
//            instance.applicationStartDefaultAllocate.put(appid, true);
//            LOG.info("!!! 开始默认调度");
//            return -1;
//        }
        HashSet<Task> delset = new HashSet<Task>();
        for (Task t : instance.applicationAssignedTaskPool.get(appid)) {
            if (result >= avaliableContainers) break;
            if (t.locations.contains(nid) && t.appid.equals(appid)) {
                if (t.pre_locations != null && !t.pre_locations.contains(nodeid)) {
                    boolean findSparePreNode = false;
                    //检查是否存在空闲的pre_loc
                    for (String str : t.pre_locations) {
                        int selectNodeRestContainer = nodeMap.get(str).nowResource.memory / requestCapability;   //检查选择队列现有的空闲container，如果空闲container >= 队列中的任务，则不需要转移
                        if (str.equals(t.nowQueueId)) {
                            if (selectNodeRestContainer > nodeTaskQueue.get(str).indexOf(t)) {
                                findSparePreNode = true;
                                break;
                            }
                        } else {
                            if (selectNodeRestContainer > nodeTaskQueue.get(str).size()) {
                                findSparePreNode = true;
                                break;
                            }
                        }
                    }
                    if (findSparePreNode) {
                        continue;
                    }
                }
                boolean findMoreFitNode = false;
                for (String loc : t.locations) {
                    if (loc.equals(nid)) continue;
                    int selectNodeRestContainer = nodeMap.get(loc).nowResource.memory / requestCapability;
                    if (selectNodeRestContainer >= (avaliableContainers - result))  {
                        if (selectNodeRestContainer >= nodeTaskQueue.get(loc).size()) {
                            findMoreFitNode = true;
                            break;
                        }
                    }
                }
                if (findMoreFitNode) continue;
                t.status = TaskStatus.ALLOCATED;
                LOG.info("!!! Task转移: " + t.no + " 从 " + t.nowQueueId + " 转移至 " + nodeid);
                delset.add(t);
                instance.nodeTaskQueue.get(t.nowQueueId).remove(t);
                instance.nodeTaskQueue.get(nid).add(t);
                t.nowQueueId = nid;
                result++;
            }
        }
        for (Task t : delset) {
            instance.applicationAssignedTaskPool.get(appid).remove(t);
        }
        if (result < avaliableContainers) {
            //还少就去接非本地性的任务
            int rest = avaliableContainers - result;
            int add = 0;
            double avg = (double)instance.applicationAssignedTaskPool.get(appid).size() / (double)nodeMap.keySet().size();
            while (add < rest) {
                //每次挑最长队列的任务接过来
                int maxtasknum = 0;
                ArrayList<Task> selectQueue = null;
                String selectNode = null;
                for (Map.Entry<String, ArrayList<Task>> entry : instance.nodeTaskQueue.entrySet()) {
                    if (entry.getKey().equals(nid)) continue;
                    if (entry.getValue().size() > maxtasknum) {
                        Node selectN = nodeMap.get(entry.getKey());
                        int selectNodeRestContainer = selectN.nowResource.memory / requestCapability;   //检查选择队列现有的空闲container，如果空闲container >= 队列中的任务，则不需要转移
                        if (selectNodeRestContainer >= entry.getValue().size()) {
                            LOG.info("!!!! " + entry.getKey() + " 上container足够，无需转移成非本地任务!");
                            continue;
                        }
                        maxtasknum = entry.getValue().size();
                        selectQueue = entry.getValue();
                        selectNode = entry.getKey();
                    }
                }
                if (selectQueue != null) {
                    for (int i = selectQueue.size() - 1; i >= 0; i--) {
                        //从后往前找
                        Task t = selectQueue.get(i);
                        if (t.appid.equals(appid) && t.status == TaskStatus.ASSIGNED && !t.locations.contains(nid)) {
                            selectQueue.remove(t);
                            t.status = TaskStatus.ALLOCATED;
                            instance.nodeTaskQueue.get(nid).add(t);
                            t.nowQueueId = nid;
                            instance.applicationAssignedTaskPool.get(appid).remove(t);
                            result++;
                            add++;
                            if (!t.locations.contains(nid)) {
                                //非本地
                                LOG.info("!!!! Node " + nid + " 从 Node " + selectNode + " 上转移了Task " + t.no + " 变为非本地");
                                t.isDataLocal = false;
                                Node targetNode = nodeMap.get(nid);
                                String src = null;
                                int mints = Integer.MAX_VALUE;
                                for (String loc : t.locations) {
                                    if (nodeMap.containsKey(loc)) {
                                        if (nodeMap.get(loc).totalTransports < mints) {
                                            mints = nodeMap.get(loc).totalTransports;
                                            src = loc;
                                        }
                                    }
                                }
                                if (src != null) {
                                    Node srcNode = nodeMap.get(src);
                                    String blkid = getBlockLocationID(t.locations, nid);
                                    if (targetNode.transportTarget.get(blkid) == null) {
                                        HashSet<String> transport = new HashSet<String>();
                                        transport.add(srcNode.nodeId);
                                        targetNode.transportTarget.put(blkid, transport);
                                    } else {
                                        HashSet<String> transport = targetNode.transportTarget.get(blkid);
                                        transport.add(srcNode.nodeId);
                                    }
                                    targetNode.totalTransports++;
                                    srcNode.totalTransports++;
                                } else {
                                    LOG.info("!!! ERROR: src 找不到:" + t.locations.toString());
                                }
                            }
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        if (result == 0 && avaliableContainers > 0) {
            instance.applicationIdleTime.put(appid, instance.applicationIdleTime.get(appid) - 1);
        }
        Node node = nodeMap.get(nodeid);
        node.nowResource.core = node.nowResource.core - result;
        node.nowResource.memory = node.nowResource.memory - (requestCapability * result);
        return result;
    }

    public void reportAllocatedContainerNumber(String nodeid, String appid, int allocatedContainers) {

        if (instance.containerPlan.get(formatAppId(appid)) != null) {
            if (instance.containerPlan.get(formatAppId(appid)).get(formatNodeId(nodeid)) != null) {
                MajorContainerPlan plan = instance.containerPlan.get(formatAppId(appid)).get(formatNodeId(nodeid));
                plan.assignedContainerNum += allocatedContainers;
                plan.containerNumToAssign -= allocatedContainers;
                LOG.info("| 应用 " + formatAppId(appid) + " 在节点 " + formatNodeId(nodeid) + " 上的分配情况: " + plan.assignedContainerNum + "|" + plan.containerNumToAssign);
            }
        }
    }

    public void updateContainerRunningTime(String appid, String cid, String host) {
        LOG.info("!!! updateContainerRunningTime: " + appid + "|" + cid + "|" + host);
        appid = formatAppId(appid);
        if (instance.applicationContainerTaskMap.containsKey(appid)) {
            LOG.info("!!!! updateContainerRunningTime2: " + cid);
            if (instance.applicationContainerTaskMap.get(appid).containsKey(cid)) {
                Task t = instance.applicationTaskMap.get(appid).get(instance.applicationContainerTaskMap.get(appid).get(cid));
                LOG.info("!!!! updateContainerRunningTime3: " + t.no);
                if (t != null) {
                    String taskid = t.no;
                    if (instance.applicationTaskStartTime.get(appid).containsKey(taskid)) {
                        long start = instance.applicationTaskStartTime.get(appid).get(taskid);
                        long end = System.currentTimeMillis();
                        instance.applicationTaskRunningTime.get(appid).put(taskid, end - start);
                        LOG.info("!!!! Task " + taskid + " | " + host + " | " + (end - start));
                    } else {
                        LOG.info("@@@@ 没找到taskid " + taskid + "|" + host);
                    }
                }

            }
        }
    }

    public void doneApplication(String appid) {
        String apid = formatAppId(appid);
        int unlocalitynumber = instance.applicationUnlocalityNumber.containsKey(apid)? instance.applicationUnlocalityNumber.get(apid) : 0;
        LOG.info("| 应用 " + apid + " 已完成，有 " + unlocalitynumber + " 个Task非本地性");
        ArrayList<String> sortedTaskIds = new ArrayList<String>(instance.applicationTaskMap.get(apid).keySet());
        Collections.sort(sortedTaskIds);
        long start = instance.applicationStartTime.get(apid);
        for (String str : sortedTaskIds) {
            String host = instance.applicationTaskFinalNode.get(apid).get(str);
            if (!instance.applicationTaskRunningTime.get(apid).containsKey(str) || !instance.applicationTaskStartTime.get(apid).containsKey(str)) continue;
            long runtime = instance.applicationTaskRunningTime.get(apid).get(str);
            long startt = instance.applicationTaskStartTime.get(apid).get(str);
            LOG.info("!!!!! Task runtime:" + str + " " + host + "|" + (startt - start) + "|" + runtime);
        }
        LOG.info("||||||||||||||||||||||||||||||||||||||华丽的应用分割线||||||||||||||||||||||||||||||||||||||");
        if (instance.applicationSplitPool.containsKey(apid)) {
            instance.applicationSplitPool.remove(apid);
        }
        if (instance.applicationStartTime.containsKey(apid)) {
            long now = System.currentTimeMillis();

            int num = instance.applicationAMGetNumber.get(apid);
            int DNnum = instance.applicationChooseDNNumber.get(apid);
            int idlenum = instance.applicationIdleTime.get(apid);
            int allnum = instance.appMapNumer.get(apid);
            StatLOG.info((now - start) + "|" + unlocalitynumber + "|" + allnum);
            instance.applicationStartTime.remove(apid);
        }
        if (instance.applicationUnlocalityNumber.containsKey(apid)) {
            instance.applicationUnlocalityNumber.remove(apid);
        }
        if (instance.applicationTaskMap.containsKey(apid)) {
            instance.applicationTaskMap.remove(apid);
        }
        if (instance.applicationUnassignedTaskPool.containsKey(apid)) {
            instance.applicationUnassignedTaskPool.remove(apid);
        }
        if (instance.applicationSplitMap.containsKey(apid)) {
            instance.applicationSplitMap.remove(apid);
        }
        if (instance.applicationSplitPool.containsKey(apid)) {
            instance.applicationSplitPool.remove(apid);
        }
        if (instance.applicationAllocater.containsKey(apid)) {
            instance.applicationAllocater.remove(apid);
        }
        if (instance.applicationAMGetNumber.containsKey(apid)) {
            instance.applicationAMGetNumber.remove(apid);
        }
        if (instance.applicationChooseDNNumber.containsKey(apid)) {
            instance.applicationChooseDNNumber.remove(apid);
        }
        if (instance.applicationAssignedTaskPool.containsKey(apid)) {
            instance.applicationAssignedTaskPool.remove(apid);
        }
        if (instance.applicationStartDefaultAllocate.containsKey(apid)) {
            instance.applicationStartDefaultAllocate.remove(apid);
        }
        if (instance.applicationIdleTime.containsKey(apid)) {
            instance.applicationIdleTime.remove(apid);
        }
        if (instance.applicationSplitChain.containsKey(apid)) {
            instance.applicationSplitChain.remove(apid);
        }
        if (instance.appMapNumer.containsKey(apid)) {
            instance.appMapNumer.remove(apid);
        }
        if (instance.applicationTaskRunningTime.containsKey(apid)) {
            instance.applicationTaskRunningTime.remove(apid);
        }
        if (instance.applicationTaskStartTime.containsKey(apid)) {
            instance.applicationTaskStartTime.remove(apid);
        }
    }

    public void updateNodeAvaliableMemory(String nodeid, int memory, int core, int totalMemory, int totlaCore) {
        String nid = formatNodeId(nodeid);
        if (!nodeInfo.containsKey(nid)) {
            MajorNodeInfo ninfo = new MajorNodeInfo(nid);
            nodeInfo.put(nid, ninfo);
        }
        MajorNodeInfo nodeinfo = nodeInfo.get(nid);
        nodeinfo.memory = memory;
        nodeinfo.lastReportTime = System.currentTimeMillis();
//        LOG.info("|||| " + nid + " update.Memory:" + memory + "|" + nodeinfo.lastReportTime);

        //NEW
        if (!nodeMap.containsKey(nodeid)) {
            Node newnode = new Node(nodeid, new Resource(totlaCore, totalMemory));
            nodeMap.put(nodeid, newnode);
        }
        if (!nodeTaskQueue.containsKey(nodeid)) {
            nodeTaskQueue.put(nodeid, new ArrayList<Task>());
        }
        Node nownode = nodeMap.get(nodeid);
        nownode.nowResource = new Resource(core, memory);
        nownode.lastUpdateTime = System.currentTimeMillis();
    }

    public void updateTaskInfo(String appid, Map<String,List<String>> taskinfo, int totalContainers, int core, int memory) {
        LOG.info("=====updateTaskInfo: " + appid);
        appid = formatAppId(appid);
        HashSet<Task> extraTasks = new HashSet<Task>();
        if (!instance.applicationTaskHostMap.containsKey(appid)) {
            instance.applicationTaskHostMap.put(appid, new HashMap<String, ArrayList<String>>());
        }
        if (!instance.applicationHostTaskMap.containsKey(appid)) {
            instance.applicationHostTaskMap.put(appid, new HashMap<String, ArrayList<String>>());
        }
        HashMap<String, ArrayList<String>> taskhostmap = instance.applicationTaskHostMap.get(appid);
        HashMap<String, ArrayList<String>> hosttaskmap = instance.applicationHostTaskMap.get(appid);
        for (String taskid: taskinfo.keySet()) {
            String str = taskid + ": ";
            ArrayList<String> taskhosts = new ArrayList<String>();
            for (String host : taskinfo.get(taskid)) {
                taskhosts.add(formatNodeId(host));
                str += host + "|";
            }
            taskhostmap.put(taskid, taskhosts);
            LOG.info("##### Taskinfo: " + str);
        }
        //add taskid to hosttaskmap
        for (String taskid: taskinfo.keySet()) {
            //clear old data
            for (String host : hosttaskmap.keySet()) {
                ArrayList<String> tasks = hosttaskmap.get(formatNodeId(host));
                if (tasks.contains(taskid)) {
                    tasks.remove(taskid);
                }
            }
            // add new data
            for (String host : taskinfo.get(taskid)) {
                if (!hosttaskmap.containsKey(formatNodeId(host))) {
                    hosttaskmap.put(host, new ArrayList<String>());
                }
                hosttaskmap.get(formatNodeId(host)).add(taskid);
            }
        }
        //NEW
        if (!instance.applicationTaskMap.containsKey(appid)) {
            instance.applicationTaskMap.put(appid, new HashMap<String, Task>());
        }
        if (!instance.applicationUnassignedTaskPool.containsKey(appid)) {
            instance.applicationUnassignedTaskPool.put(appid, new HashSet<Task>());
        }
        HashMap<String, Task> taskMap = instance.applicationTaskMap.get(appid);
        HashSet<Task> taskpool = instance.applicationUnassignedTaskPool.get(appid);
        for (Map.Entry<String, List<String>> entry : taskinfo.entrySet()) {
            HashSet<String> loc = new HashSet<String>();
            for (String nodeid : entry.getValue()) {
                loc.add(nodeid);
                if (nodeMap.containsKey(nodeid)) {
                    Node node = nodeMap.get(nodeid);
                    node.allocatedTasks.add(entry.getKey());
                }
            }
            if (taskMap.containsKey(entry.getKey())) {
                Task t = taskMap.get(entry.getKey());
                t.locations = loc;
            } else {
                Task ntask = new Task(entry.getKey(), new Resource(core, memory) , loc, appid);
                taskMap.put(entry.getKey(), ntask);
                taskpool.add(ntask);
                if (!entry.getKey().endsWith("_0")) {
                    LOG.info("Extra task: " + entry.getKey());
                    extraTasks.add(ntask);
                }
            }
        }
        for (Task t : extraTasks) {
            instance.appMapNumer.put(appid, instance.appMapNumer.get(appid) + 1);
            String id = t.no;
            String ids = id.substring(0, id.length() - 1) + "0";
            if (instance.applicationTaskMap.get(appid).containsKey(ids)) {
                if (instance.applicationTaskMap.get(appid).get(ids).pre_locations != null) {
                    t.pre_locations = new HashSet<String>(instance.applicationTaskMap.get(appid).get(ids).pre_locations);
                }
            }
        }
        //处理SplitChain
        //排序task
        ArrayList<String> sortedTaskIds = new ArrayList<String>(instance.applicationTaskMap.get(appid).keySet());
        Collections.sort(sortedTaskIds);
        for (String str : sortedTaskIds) {
            LOG.info("DEBUG!!! " + str);
        }
        Set<String> taskids = instance.applicationTaskMap.keySet();

        HashSet<SplitChain> spcs = instance.applicationSplitChain.get(appid);
        for (SplitChain spc : spcs) {
            if (spc.isFound) continue;
            HashSet<String> startloc = spc.fileStartSplit.location;
            //找startSplit对应的Task
            for (String tid : sortedTaskIds) {
                Task t = taskMap.get(tid);
                HashSet<String> re = new HashSet<String>();
                re.addAll(t.locations);
                re.retainAll(startloc);
                if (re.size() == t.locations.size()) {
                    //找到start
                    if (sortedTaskIds.indexOf(tid) + spc.targetIndex >= sortedTaskIds.size()) continue;
                    Task targetTask = taskMap.get(sortedTaskIds.get(sortedTaskIds.indexOf(tid) + spc.targetIndex));
                    HashSet<String> re2 = new HashSet<String>();
                    re2.addAll(spc.targetSplit.location);
                    re2.retainAll(targetTask.locations);
                    if (re2.size() == targetTask.locations.size()) {
                        //找到target
                        if (sortedTaskIds.indexOf(tid) + spc.targetIndex + 1 >= sortedTaskIds.size()) continue;
                        Task chainTask = taskMap.get(sortedTaskIds.get(sortedTaskIds.indexOf(tid) + spc.targetIndex + 1));
                        HashSet<String> re3 = new HashSet<String>();
                        re3.addAll(spc.chainSplit.location);
                        re3.retainAll(chainTask.locations);
                        if (re3.size() == chainTask.locations.size()) {
                            //正确！
                            targetTask.pre_locations = new HashSet<String>();
                            targetTask.pre_locations.addAll(spc.locations);
                            spc.isFound = true;
                            LOG.info("!!!debug: " + targetTask.no + " pre_locations:" + targetTask.pre_locations.toString());
                            break;
                        }
                    }
                }
            }
        }

        //START ALLOCATE
        if (instance.applicationAllocater.containsKey(appid)) {
//            Allocater a = (Allocater)instance.applicationAllocater.get(appid);
//            if (!a.isStart) {
                Thread newa = new Allocater(appid);
                instance.applicationAllocater.put(appid, newa);
                LOG.info("!!!Allocater start!!!");
                newa.start();
                LOG.info("!!!Allocater starting!!!");
//            }
        }
    }

    public class Allocater extends Thread {
        public String appid;
        public boolean isStart;
        public Allocater (String id) {
            appid = id;
            isStart = false;
        }

        //-------------------------------------------------------------------------
       //自写函数
        public void allocateRestTask() {
            LOG.info("!!!!!!!!进入尾部调用---------");
            //本方法用于处理尾部剩余Task
            HashMap<String, HashSet<String>> nodeRestTaskMap = new HashMap<String, HashSet<String>>();
            HashMap<String, HashSet<String>> nodeTaskMap = new HashMap<String, HashSet<String>>();
            HashMap<String, Task> taskMap = new HashMap<String, Task>();
            HashMap<String, Integer> nodeTotalContainer = new HashMap<String, Integer>();
            HashMap<String, Integer> nodeNowContainer = new HashMap<String, Integer>();
            HashMap<String, Double> nodeZ = new HashMap<String, Double>();
            HashMap<String, Double> taskZ = new HashMap<String, Double>();
            int taskcore = 0;
            int taskmemory = 0;
            int assignedTask = 0;
            int totalNodeContainer = 0; //所有node最大容量之和
            int avgTask = 0;
            LOG.info("!!!! Application " + appid + " 开始分配");
            if (instance.applicationUnassignedTaskPool.containsKey(appid)) {
                if (instance.applicationUnassignedTaskPool.get(appid).size() != 0) {
                    for (Task task : instance.applicationUnassignedTaskPool.get(appid)) {
                        taskcore = task.request.core;
                        taskmemory = task.request.memory;
                        break;
                    }
                } else {
                    return;
                }
            } else {
                instance.applicationUnassignedTaskPool.put(appid, new HashSet<Task>());
                return;
            }
            if (taskcore == 0 || taskmemory == 0) {
                //error?
                return;
            }
            for (Node node : instance.nodeMap.values()) {
                nodeTaskMap.put(node.nodeId, new HashSet<String>());
                nodeRestTaskMap.put(node.nodeId, new HashSet<String>());
                int totalcontainer = Math.min(node.totalResource.core / taskcore, node.totalResource.memory / taskmemory);
                nodeTotalContainer.put(node.nodeId, totalcontainer);
                totalNodeContainer += totalcontainer;
                nodeNowContainer.put(node.nodeId, 0);
                LOG.info("!!!!! Node " + node.nodeId + " totalResource = " + totalcontainer);
            }

            //-----------------------------------------------------------------------
            LOG.info("!!!!!ALL Node  totalResource = " + totalNodeContainer);
            //---------------------------------------------------------------------------------------------
            avgTask = Math.max(instance.applicationUnassignedTaskPool.get(appid).size() / instance.nodeMap.keySet().size(), 1);
            for (Task task : instance.applicationUnassignedTaskPool.get(appid)) {
                taskMap.put(task.no, task);
                for (String nodeid : task.locations) {
                    if (nodeTaskMap.containsKey(nodeid)) {
                        nodeTaskMap.get(nodeid).add(task.no);
                    }
                    if (nodeRestTaskMap.containsKey(nodeid)) {
                        nodeRestTaskMap.get(nodeid).add(task.no);
                    }
                }
            }
            LOG.info("!!!!! TaskPool: " + instance.applicationUnassignedTaskPool.get(appid).size());
            //update Z
            for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                String nodeid = entry.getKey();
                HashSet<String> nodetasks = entry.getValue();
                Double Z = ((double)nodetasks.size()) / ((double)nodeTotalContainer.get(nodeid));
                nodeZ.put(nodeid, Z);
            }
            for (Task task : taskMap.values()) {
                Double Z = 0.0;
                int count = 0;
                for (String loc : task.locations) {
                    if (nodeZ.containsKey(loc)) {
                        Z+= nodeZ.get(loc);
                        count++;
                    }
                }
                Z = Z / (double)count;
                taskZ.put(task.no, Z);
            }
            for (Map.Entry<String, Double> entry : nodeZ.entrySet()) {
                LOG.info("!!!! nodeZ: " + entry.getKey() + "|" + nodeTaskMap.get(entry.getKey()).size() + "|" + entry.getValue());
            }
            HashSet<String> allocatedNode = new HashSet<String>();
            for (int i = 0; i < nodeZ.keySet().size(); i++) {
                //find node
                String finalNode = null;
                Double finalZ = 0.0;
                boolean hasSpareNode = false;   //sparenode 定义为Z值<=1的node，用于划分Z<=1和Z>1的情况
                for (Map.Entry<String, Double> entry : nodeZ.entrySet()) {
                    if (allocatedNode.contains(entry.getKey())) continue;
                    if (hasSpareNode) {
                        if (entry.getValue() < finalZ && entry.getValue() != 0.0) {
                            finalNode = entry.getKey();
                            finalZ = entry.getValue();
                        }
                    } else {
                        if (entry.getValue() <= 1 && entry.getValue() != 0.0) {
                            hasSpareNode = true;
                            finalNode = entry.getKey();
                            finalZ = entry.getValue();
                        } else {
                            if (finalNode == null && entry.getValue() != 0.0) {
                                finalNode = entry.getKey();
                                finalZ = entry.getValue();
                            } else {
                                if (entry.getValue() > finalZ && entry.getValue() != 0.0) {
                                    finalZ = entry.getValue();
                                    finalNode = entry.getKey();
                                }
                            }
                        }
                    }
                }
                //start allocate
                if (finalNode != null) {
                    LOG.info("!!!!! finalNode: " + finalNode + " hasSpareNode = " + hasSpareNode);
                    allocatedNode.add(finalNode);
                    LOG.info("!!!!!!hasNoSpareNode:" + nodeTotalContainer.get(finalNode));
                    for (int j = 0; j < Math.min(Math.min(nodeTotalContainer.get(finalNode), avgTask), nodeTaskMap.get(finalNode).size()); j++) {
                        Double maxTaskZ = Double.MIN_VALUE;
                        String selectTask = null;
                        for (String tid : nodeTaskMap.get(finalNode)) {
                            Task t = taskMap.get(tid);
                            if (t.status != TaskStatus.UNASSIGNED) continue;
                            if (t.pre_locations != null) {
                                if (t.pre_locations.contains(finalNode)) {
                                    maxTaskZ = Double.MAX_VALUE;
                                    selectTask = tid;
                                }
                            } else {
                                if (taskZ.get(tid) > maxTaskZ) {
                                    maxTaskZ = taskZ.get(tid);
                                    selectTask = tid;
                                }
                            }
                        }
                        if (selectTask == null) continue;
                        Task t = taskMap.get(selectTask);
                        t.status = TaskStatus.ASSIGNED;
                        instance.applicationAssignedTaskPool.get(appid).add(t);
                        for (String loc : t.locations) {
                            if (!loc.equals(finalNode)) {
                                nodeTaskMap.get(loc).remove(selectTask);
                            }
                            nodeRestTaskMap.get(loc).remove(selectTask);
                        }
                        nodeTaskQueue.get(finalNode).add(t);
                        t.nowQueueId = finalNode;
                        instance.applicationUnassignedTaskPool.get(appid).remove(t);
                        nodeRestTaskMap.get(finalNode).remove(t.no);
                        t.isDataLocal = true;
                        nodeNowContainer.put(finalNode, nodeNowContainer.get(finalNode) + 1);
                        LOG.info("!!!!! Task " + t.no + " 分配到了 " + finalNode + " 上");
                        assignedTask++;
                        //update Z
                        for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                            String nodeid = entry.getKey();
                            HashSet<String> nodetasks = entry.getValue();
                            Double Z = ((double)nodetasks.size()) / ((double)nodeTotalContainer.get(nodeid));
                            nodeZ.put(nodeid, Z);
                        }
                        for (Task task : taskMap.values()) {
                            Double Z = 0.0;
                            int count = 0;
                            for (String loc : task.locations) {
                                if (nodeZ.containsKey(loc)) {
                                    Z+= nodeZ.get(loc);
                                    count++;
                                }
                            }
                            Z = Z / (double)count;
                            taskZ.put(task.no, Z);
                        }
                    }
                } else {
                    LOG.info("!!!!! finalNode: not found!");
                }
                //update Z
                for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                    String nodeid = entry.getKey();
                    HashSet<String> nodetasks = entry.getValue();
                    Double Z = ((double)nodetasks.size()) / ((double)nodeTotalContainer.get(nodeid));
                    nodeZ.put(nodeid, Z);
                }
                for (Task task : taskMap.values()) {
                    Double Z = 0.0;
                    int count = 0;
                    for (String loc : task.locations) {
                        if (nodeZ.containsKey(loc)) {
                            Z+= nodeZ.get(loc);
                            count++;
                        }
                    }
                    Z = Z / (double)count;
                    taskZ.put(task.no, Z);
                }
            }
            //现在的情况是，所有节点都分配了最多avgtask个Task，接下来要填补空缺尽可能让所有节点分配到的Task数量相等
            //先填有preloc的
            HashSet<Task> rmset = new HashSet<Task>();
            for (Task t : instance.applicationUnassignedTaskPool.get(appid)) {
                if (t.status != TaskStatus.UNASSIGNED) continue;
                //找最少的
                int mintask = Integer.MAX_VALUE;
                String minNode = null;
                //先找preloc
                if (t.pre_locations != null) {
                    for (String preloc : t.pre_locations) {
                        if (nodeNowContainer.get(preloc) < avgTask) {
                            if (nodeNowContainer.get(preloc) < mintask) {
                                mintask = nodeNowContainer.get(preloc);
                                minNode = preloc;
                            }
                        }
                    }
                }
                if (minNode != null) {
                    t.status = TaskStatus.ASSIGNED;
                    instance.applicationAssignedTaskPool.get(appid).add(t);
                    for (String loc : t.locations) {
                        if (!loc.equals(minNode)) {
                            nodeTaskMap.get(loc).remove(t);
                        }
                        nodeRestTaskMap.get(loc).remove(t);
                    }
                    nodeTaskQueue.get(minNode).add(t);
                    t.nowQueueId = minNode;
                    rmset.add(t);
                    nodeRestTaskMap.get(minNode).remove(t.no);
                    t.isDataLocal = true;
                    nodeNowContainer.put(minNode, nodeNowContainer.get(minNode) + 1);
                    LOG.info("!!!!! 尾部Task0 " + t.no + " 分配到了 " + minNode + " 上");
                    assignedTask++;
                }
            }
            for (Task t : rmset) {
                instance.applicationUnassignedTaskPool.get(appid).remove(t);
            }
            rmset.clear();
            for (Task t : instance.applicationUnassignedTaskPool.get(appid)) {
                if (t.status != TaskStatus.UNASSIGNED) continue;
                //找最少的
                int maxtask = Integer.MIN_VALUE;
                String maxNode = null;
                //先找preloc
                if (t.pre_locations != null) {
                    for (String preloc : t.pre_locations) {
                        Node node = nodeMap.get(preloc);
                        int restcontainer = Math.min(node.nowResource.core / taskcore, node.nowResource.memory / taskmemory) - nodeTaskQueue.get(preloc).size();
                        if (restcontainer > 0) {
                            if (restcontainer > maxtask) {
                                maxtask = nodeNowContainer.get(preloc);
                                maxNode = preloc;
                            }
                        }
                    }
                }
                if (maxNode != null) {
                    t.status = TaskStatus.ASSIGNED;
                    instance.applicationAssignedTaskPool.get(appid).add(t);
                    for (String loc : t.locations) {
                        if (!loc.equals(maxNode)) {
                            nodeTaskMap.get(loc).remove(t);
                        }
                        nodeRestTaskMap.get(loc).remove(t);
                    }
                    nodeTaskQueue.get(maxNode).add(t);
                    t.nowQueueId = maxNode;
                    rmset.add(t);
                    nodeRestTaskMap.get(maxNode).remove(t.no);
                    t.isDataLocal = true;
                    nodeNowContainer.put(maxNode, nodeNowContainer.get(maxNode) + 1);
                    LOG.info("!!!!! 尾部Task0.5 " + t.no + " 分配到了 " + maxNode + " 上");
                    assignedTask++;
                }
            }
            for (Task t : rmset) {
                instance.applicationUnassignedTaskPool.get(appid).remove(t);
            }

            //首先遍历每个未分配的Task，如果Task本地性节点中还存在数量<avgtask的节点，则分配（是否有必要？需要考虑）
            for (Task t : instance.applicationUnassignedTaskPool.get(appid)) {
                if (t.status != TaskStatus.UNASSIGNED) continue;
                //找最少的
                int mintask = Integer.MAX_VALUE;
                String minNode = null;
                //先找preloc
                if (t.pre_locations != null) {
                    for (String preloc : t.pre_locations) {
                        if (nodeNowContainer.get(preloc) < avgTask) {
                            if (nodeNowContainer.get(preloc) < mintask) {
                                mintask = nodeNowContainer.get(preloc);
                                minNode = preloc;
                            }
                        }
                    }
                }
                if (minNode == null) {
                    for (String loc : t.locations) {
                        if (nodeNowContainer.get(loc) < avgTask) {
                            if (nodeNowContainer.get(loc) < mintask) {
                                mintask = nodeNowContainer.get(loc);
                                minNode = loc;
                            }
                        }
                    }
                }

                if (minNode != null) {
                    t.status = TaskStatus.ASSIGNED;
                    instance.applicationAssignedTaskPool.get(appid).add(t);
                    for (String loc : t.locations) {
                        if (!loc.equals(minNode)) {
                            nodeTaskMap.get(loc).remove(t);
                        }
                        nodeRestTaskMap.get(loc).remove(t);
                    }
                    nodeTaskQueue.get(minNode).add(t);
                    t.nowQueueId = minNode;
                    rmset.add(t);
                    nodeRestTaskMap.get(minNode).remove(t.no);
                    t.isDataLocal = true;
                    nodeNowContainer.put(minNode, nodeNowContainer.get(minNode) + 1);
                    LOG.info("!!!!! 尾部Task1 " + t.no + " 分配到了 " + minNode + " 上");
                    assignedTask++;
                }
            }
            for (Task t : rmset) {
                instance.applicationUnassignedTaskPool.get(appid).remove(t);
            }
            //现在剩下的所有task，都不存在未达到avg的local node
            //把剩下的task填进最短的队列中
            HashSet<Task> pool = new HashSet<Task>(instance.applicationUnassignedTaskPool.get(appid));
            for (Task t : pool) {
                int mintask = Integer.MAX_VALUE;
                String minNode = null;
                for (Map.Entry<String, Integer> entity : nodeNowContainer.entrySet()) {
                    if (entity.getValue() < mintask) {
                        minNode = entity.getKey();
                        mintask = entity.getValue();
                    } else {
                        if (entity.getValue() == mintask) {
                            if (t.locations.contains(entity.getKey())) {
                                minNode = entity.getKey();
                                mintask = entity.getValue();
                            }
                        }
                    }
                }
                if (minNode != null) {
                    t.status = TaskStatus.ASSIGNED;
                    instance.applicationAssignedTaskPool.get(appid).add(t);
                    for (String loc : t.locations) {
                        if (!loc.equals(minNode)) {
                            nodeTaskMap.get(loc).remove(t);
                        }
                        nodeRestTaskMap.get(loc).remove(t);
                    }
                    nodeTaskQueue.get(minNode).add(t);
                    t.nowQueueId = minNode;
                    instance.applicationUnassignedTaskPool.get(appid).remove(t);
                    nodeRestTaskMap.get(minNode).remove(t.no);
                    t.isDataLocal = true;
                    nodeNowContainer.put(minNode, nodeNowContainer.get(minNode) + 1);

                    assignedTask++;
                    if (!t.locations.contains(minNode)) {
                        //非本地，选择节点
                        String blkid = getBlockLocationID(t.locations, minNode);
                        Node srcNode = null;
                        Integer minTransport = Integer.MIN_VALUE;
                        for (String nodeid : t.locations) {
                            if (nodeMap.get(nodeid).totalTransports > minTransport) {
                                srcNode = nodeMap.get(nodeid);
                            }
                        }
                        if (srcNode != null) {
                            Node targetNode = nodeMap.get(minNode);
                            if (targetNode.transportTarget.get(blkid) == null) {
                                HashSet<String> transport = new HashSet<String>();
                                transport.add(srcNode.nodeId);
                                targetNode.transportTarget.put(blkid, transport);
                            } else {
                                HashSet<String> transport = targetNode.transportTarget.get(blkid);
                                transport.add(srcNode.nodeId);
                            }
                            targetNode.totalTransports++;
                            srcNode.totalTransports++;
                            LOG.info("!!!!! 尾部Task2 " + t.no + " 分配到了 " + minNode + " 上, 非本地, 从 " + srcNode + " 读取block");
                        }
                    } else {
                        LOG.info("!!!!! 尾部Task2 " + t.no + " 分配到了 " + minNode + " 上, 本地");
                    }
                }
            }
            LOG.info("!!!! 尾部Task分配完成，剩余 " + instance.applicationUnassignedTaskPool.size() + " , 分布如下:");
            String str = "";
            for (Node node : nodeMap.values()) {
                int nowc = nodeNowContainer.get(node.nodeId);
                str += node.nodeId + "|" + nowc+"--";
            }
            LOG.info("!!!! 尾部分布: " + str);
        }


//-------------------------------------------------------------
        @Override
        public void run() {
            isStart = true;
            while (true) {
                HashMap<String, HashSet<String>> nodeRestTaskMap = new HashMap<String, HashSet<String>>();
                HashMap<String, HashSet<String>> nodeTaskMap = new HashMap<String, HashSet<String>>();
                HashMap<String, Task> taskMap = new HashMap<String, Task>();
                HashMap<String, Integer> nodeTotalContainer = new HashMap<String, Integer>();
                HashMap<String, Integer> nodeNowContainer = new HashMap<String, Integer>();
                HashMap<String, Double> nodeZ = new HashMap<String, Double>();
                HashMap<String, Double> taskZ = new HashMap<String, Double>();
                int taskcore = 0;
                int taskmemory = 0;
                int assignedTask = 0;
                int totalNodeContainer = 0; //所有node最大容量之和
                LOG.info("!!!! Application " + appid + " 开始分配");
                //笔记1.统计未分配的任务task
                if (instance.applicationUnassignedTaskPool.containsKey(appid)) {
                    if (instance.applicationUnassignedTaskPool.get(appid).size() != 0) {
                        for (Task task : instance.applicationUnassignedTaskPool.get(appid)) {
                            taskcore = task.request.core;
                            taskmemory = task.request.memory;
                            break;
                        }
                    } else {
                        return;
                    }
                } else {
                    instance.applicationUnassignedTaskPool.put(appid, new HashSet<Task>());
                    return;
                }
                if (taskcore == 0 || taskmemory == 0) {
                    //error?
                    return;
                }

                //笔记2.统计所有节点的情况以及拥有的container数量
                for (Node node : instance.nodeMap.values()) {
                    nodeTaskMap.put(node.nodeId, new HashSet<String>());
                    nodeRestTaskMap.put(node.nodeId, new HashSet<String>());
                    int totalcontainer = Math.min(node.totalResource.core / taskcore, node.totalResource.memory / taskmemory);
                    nodeTotalContainer.put(node.nodeId, totalcontainer);
                    totalNodeContainer += totalcontainer;
                    nodeNowContainer.put(node.nodeId, 0);
                    LOG.info("!!!!! Node " + node.nodeId + " totalResource = " + totalcontainer);
                }
                if (instance.applicationUnassignedTaskPool.get(appid).size() < totalNodeContainer) {
                    //上来就小于，直接进尾部调度
                    allocateRestTask();
                    break;
                }

                //笔记3.各个节点统计需求它的task
                for (Task task : instance.applicationUnassignedTaskPool.get(appid)) {
                    taskMap.put(task.no, task);
                    for (String nodeid : task.locations) {
                        if (nodeTaskMap.containsKey(nodeid)) {
                            nodeTaskMap.get(nodeid).add(task.no);
                        }
                        if (nodeRestTaskMap.containsKey(nodeid)) {
                            nodeRestTaskMap.get(nodeid).add(task.no);
                        }
                    }
                }
                LOG.info("!!!!! TaskPool: " + instance.applicationUnassignedTaskPool.get(appid).size());

                //笔记4.统计节点评估值任务评估值
                //update Z
                for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                    String nodeid = entry.getKey();
                    HashSet<String> nodetasks = entry.getValue();
                    Double Z = ((double)nodetasks.size()) / ((double)nodeTotalContainer.get(nodeid));
                    nodeZ.put(nodeid, Z);
                }
                for (Task task : taskMap.values()) {
                    Double Z = 0.0;
                    int count = 0;
                    for (String loc : task.locations) {
                        if (nodeZ.containsKey(loc)) {
                            Z+= nodeZ.get(loc);
                            count++;
                        }
                    }
                    Z = Z / (double)count;
                    taskZ.put(task.no, Z);
                }

                //笔记5.选定先要分配的节点
                HashSet<String> allocatedNode = new HashSet<String>();//标记节点是否已经分配完成
                for (int i = 0; i < nodeZ.keySet().size(); i++) {
                    //find node
                    String finalNode = null;
                    Double finalZ = 0.0;
                    boolean hasSpareNode = false;   //sparenode 定义为Z值<=1的node，用于划分Z<=1和Z>1的情况
                    for (Map.Entry<String, Double> entry : nodeZ.entrySet()) {
                        if (allocatedNode.contains(entry.getKey())) continue;
                        if (hasSpareNode) {//存在空闲节点
                            if (entry.getValue() < finalZ) {//当前找到的节点评估值比刚才暂存的还要小改变final节点
                                finalNode = entry.getKey();
                                finalZ = entry.getValue();
                            }
                        } else {//还未找到空闲节点
                            if (entry.getValue() <= 1) {//节点评估值小于等于1，任务可以放进
                                hasSpareNode = true;
                                finalNode = entry.getKey();
                                finalZ = entry.getValue();
                            } else {//节点评估值大于1，任务不能完全放入
                                if (finalNode == null) {//选定的node
                                    finalNode = entry.getKey();
                                    finalZ = entry.getValue();
                                } else {//存在选定的节点，那么就选取节点品估值最大的
                                    if (entry.getValue() > finalZ) {
                                        finalZ = entry.getValue();
                                        finalNode = entry.getKey();
                                    }
                                }
                            }
                        }
                    }
                    //start allocate
                    if (finalNode != null) {
                        LOG.info("!!!!! finalNode: " + finalNode + " hasSpareNode = " + hasSpareNode);
                        allocatedNode.add(finalNode);
                        if (hasSpareNode) {//有满足节点评估值小于1的情况
                            //nodeZ <= 1
                            LOG.info("!!!!!!hasSpareNode:" + nodeTaskMap.get(finalNode).size());
                            HashSet<String> toRemoveTask = new HashSet<String>();   //用于事先去除SplitChain的Task
                            for (String tid : nodeTaskMap.get(finalNode)) {
                                Task t = taskMap.get(tid);
                                if (t.pre_locations != null && !t.pre_locations.contains(finalNode)) {
                                    toRemoveTask.add(t.no);
                                    continue;
                                }
                                for (String loc : t.locations) {
                                    if (!loc.equals(finalNode)) {
                                        nodeTaskMap.get(loc).remove(tid);
                                    }
                                    nodeRestTaskMap.get(loc).remove(tid);
                                }
                                t.status = TaskStatus.ASSIGNED;
                                instance.applicationAssignedTaskPool.get(appid).add(t);
                                nodeTaskQueue.get(finalNode).add(t);
                                t.nowQueueId = finalNode;
                                instance.applicationUnassignedTaskPool.get(appid).remove(t);
                                nodeRestTaskMap.get(finalNode).remove(t.no);
                                t.isDataLocal = true;
                                LOG.info("!!!!! Task " + t.no + " 分配到了 " + finalNode + " 上");
                                assignedTask++;
                            }
                            for (String s : toRemoveTask) {
                                nodeTaskMap.get(finalNode).remove(s);//移除掉已经分配的任务
                            }
                        } else {//无满足节点评估值小于1的情况。
                            //nodeZ > 1
                            LOG.info("!!!!!!hasNoSpareNode:" + nodeTotalContainer.get(finalNode));
                            for (int j = 0; j < nodeTotalContainer.get(finalNode); j++) {
                                Double maxTaskZ = Double.MIN_VALUE;
                                String selectTask = null;
                                boolean selectTaskHasPreLoc = false;
                                //选z最大的task
                                for (String tid : nodeTaskMap.get(finalNode)) {
                                    Task t = taskMap.get(tid);
                                    if (t.status != TaskStatus.UNASSIGNED) continue;
                                    if (t.pre_locations != null && t.pre_locations.contains(finalNode)) {
                                        maxTaskZ = Double.MAX_VALUE;
                                        selectTask = tid;
                                    } else {
                                        if (selectTaskHasPreLoc) {
                                            if (t.pre_locations != null) {
                                                if (taskZ.get(tid) > maxTaskZ) {
                                                    maxTaskZ = taskZ.get(tid);
                                                    selectTask = tid;
                                                }
                                            } else {
                                                maxTaskZ = taskZ.get(tid);
                                                selectTask = tid;
                                                selectTaskHasPreLoc = false;
                                            }
                                        } else {
                                            if (t.pre_locations == null) {
                                                if (taskZ.get(tid) > maxTaskZ) {
                                                    maxTaskZ = taskZ.get(tid);
                                                    selectTask = tid;
                                                }
                                            }
                                        }

                                    }
                                }
                                if (selectTask == null) continue;
                                Task t = taskMap.get(selectTask);
                                t.status = TaskStatus.ASSIGNED;
                                instance.applicationAssignedTaskPool.get(appid).add(t);
                                for (String loc : t.locations) {
                                    if (!loc.equals(finalNode)) {
                                        nodeTaskMap.get(loc).remove(selectTask);
                                    }
                                    nodeRestTaskMap.get(loc).remove(selectTask);
                                }
                                nodeTaskQueue.get(finalNode).add(t);
                                t.nowQueueId = finalNode;
                                instance.applicationUnassignedTaskPool.get(appid).remove(t);
                                nodeRestTaskMap.get(finalNode).remove(t.no);
                                t.isDataLocal = true;
                                LOG.info("!!!!! Task " + t.no + " 分配到了 " + finalNode + " 上");
                                assignedTask++;
                                //update Z
                                for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                                    String nodeid = entry.getKey();
                                    HashSet<String> nodetasks = entry.getValue();
                                    Double Z = ((double)nodetasks.size()) / ((double)nodeTotalContainer.get(nodeid));
                                    nodeZ.put(nodeid, Z);
                                }
                                for (Task task : taskMap.values()) {
                                    Double Z = 0.0;
                                    int count = 0;
                                    for (String loc : task.locations) {
                                        if (nodeZ.containsKey(loc)) {
                                            Z+= nodeZ.get(loc);
                                            count++;
                                        }
                                    }
                                    Z = Z / (double)count;
                                    taskZ.put(task.no, Z);
                                }
                            }
                        }
                    } else {//找不到选定节点
                        LOG.info("!!!!! finalNode: not found!");
                    }
                    //update Z
                    for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                        String nodeid = entry.getKey();
                        HashSet<String> nodetasks = entry.getValue();
                        Double Z = ((double)nodetasks.size()) / ((double)nodeTotalContainer.get(nodeid));
                        nodeZ.put(nodeid, Z);
                    }
                    for (Task task : taskMap.values()) {
                        Double Z = 0.0;
                        int count = 0;
                        for (String loc : task.locations) {
                            if (nodeZ.containsKey(loc)) {
                                Z+= nodeZ.get(loc);
                                count++;
                            }
                        }
                        Z = Z / (double)count;
                        taskZ.put(task.no, Z);
                    }
                }
                LOG.info("!!!!!!贪心共分配了 " + assignedTask + " 个任务");
                //部分2
                //part 2
                int restTask = instance.applicationUnassignedTaskPool.get(appid).size();
                if (restTask == 0) {
                    LOG.info("!!!!!!贪心分配完毕");
                    return;
                }
                LOG.info("!!!!!!!贪心未分配完任务，剩余 " + restTask + " 个任务");
                //统计container是否有剩余
                HashSet<Node> spareNode = new HashSet<Node>();
                int spareContainer = 0;
                for (Map.Entry<String, HashSet<String>> entry : nodeTaskMap.entrySet()) {
                    String nodeid = entry.getKey();
                    HashSet<String> nodetasks = entry.getValue();
                    if (nodeTotalContainer.get(nodeid) > nodetasks.size()) {
                        spareNode.add(nodeMap.get(nodeid));
                        spareContainer += nodeTotalContainer.get(nodeid) - nodetasks.size();
                    }
                }
                if (spareContainer > 0) {   //如果Container还有剩
                    //补上由于splitchain未能分配到二级本地性的Task
                    for (Task t : instance.applicationUnassignedTaskPool.get(appid)) {
                        String finalNode = null;
                        for (String nodeid : t.locations) {
                            if (spareNode.contains(nodeMap.get(nodeid))) {
                                finalNode = nodeid;
                                break;
                            }
                        }
                        if (finalNode != null) {
                            nodeTaskMap.get(finalNode).add(t.no);
                            spareContainer--;
                            if (nodeTotalContainer.get(finalNode) <= nodeTaskMap.get(finalNode).size()) {
                                spareNode.remove(finalNode);
                            }
                            for (String loc : t.locations) {
                                if (!loc.equals(finalNode)) {
                                    nodeTaskMap.get(loc).remove(t.no);
                                }
                                nodeRestTaskMap.get(loc).remove(t.no);
                            }
                            t.status = TaskStatus.ASSIGNED;
                            instance.applicationAssignedTaskPool.get(appid).add(t);
                            nodeTaskQueue.get(finalNode).add(t);
                            t.nowQueueId = finalNode;
                            instance.applicationUnassignedTaskPool.get(appid).remove(t);
                            nodeRestTaskMap.get(finalNode).remove(t.no);
                            t.isDataLocal = true;
                            LOG.info("!!!!! Task " + t.no + " 分配到了 " + finalNode + " 上，补分配");
                            assignedTask++;
                        }
                    }
                }
                if (spareContainer > 0) {   //如果Container还有剩
                    //计算剩余Task的Z
                    for (Map.Entry<String, HashSet<String>> entry : nodeRestTaskMap.entrySet()) {
                        double z = ((double)entry.getValue().size()) / ((double)nodeTotalContainer.get(entry.getKey()));
                        nodeZ.put(entry.getKey(), z);
                    }
                    for (Task t : instance.applicationUnassignedTaskPool.get(appid)) {
                        double z = 0;
                        int count = 0;
                        for (String loc : t.locations) {
                            z += nodeZ.get(loc);
                            count++;
                        }
                        z = z / (double)count;
                        taskZ.put(t.no, z);
                    }

                    for (int i = 0; i < Math.min(restTask, spareContainer); i++) {
                        //挑选有空余container的节点中数据传输最少的节点
                        Integer minTransports = Integer.MAX_VALUE;
                        HashSet<Node> minTransportNodes = new HashSet<Node>();
                        for (Node node : spareNode) {
                            if (node.totalTransports < minTransports) {
                                minTransports = node.totalTransports;
                                minTransportNodes.clear();
                                minTransportNodes.add(node);
                            } else {
                                if (node.totalTransports == minTransports) {
                                    minTransportNodes.add(node);
                                }
                            }
                        }
                        Integer maxSpareContainer = Integer.MIN_VALUE;
                        Node targetNode = null;
                        for (Node node : minTransportNodes) {
                            int restContainer = nodeTotalContainer.get(node.nodeId) - nodeTaskMap.get(node.nodeId).size();
                            if (restContainer > maxSpareContainer) {
                                targetNode = node;
                            }
                        }
                        if (targetNode != null) {
                            Task t = null;
                            //挑选一个Z最大的Task
                            double maxTaskZ = Double.MIN_VALUE;
                            for (Task task : instance.applicationUnassignedTaskPool.get(appid)) {

                                if (taskZ.get(task.no) > maxTaskZ) {
                                    t = task;
                                    maxTaskZ = taskZ.get(task.no);
                                } else {
                                    LOG.info("???task " + task.no + "|" + taskZ.get(task.no));
                                }
                            }
                            Node srcNode = null;
                            Integer minTransport = Integer.MIN_VALUE;
                            for (String nodeid : t.locations) {
                                if (nodeMap.get(nodeid).totalTransports > minTransport) {
                                    srcNode = nodeMap.get(nodeid);
                                }
                            }
                            if (srcNode != null && t != null) {
                                nodeTaskQueue.get(targetNode.nodeId).add(t);
                                t.nowQueueId = targetNode.nodeId;
                                t.status = TaskStatus.ASSIGNED;
                                instance.applicationUnassignedTaskPool.get(appid).remove(t);
                                instance.applicationAssignedTaskPool.get(appid).add(t);
                                String blkid = getBlockLocationID(t.locations, targetNode.nodeId);
                                for (String loc : t.locations) {
                                    nodeRestTaskMap.get(loc).remove(t.no);
                                    //更新nodeZ
                                    double z = ((double)nodeRestTaskMap.get(loc).size()) / ((double)nodeTotalContainer.get(loc));
                                    nodeZ.put(loc, z);
                                }
                                //重新计算计算剩余Task的Z
                                for (Task tt : instance.applicationUnassignedTaskPool.get(appid)) {
                                    double z = 0;
                                    int count = 0;
                                    for (String loc : tt.locations) {
                                        z += nodeZ.get(loc);
                                        count++;
                                    }
                                    z = z / (double)count;
                                    taskZ.put(tt.no, z);
                                }
                                t.isDataLocal = false;
                                LOG.info("!!!!!!!任务 " + t.no + " 分配到了 " + targetNode.nodeId + " 上，从 " + srcNode.nodeId + " 读取Block，BLKID=" + blkid);
                                if (targetNode.transportTarget.get(blkid) == null) {
                                    HashSet<String> transport = new HashSet<String>();
                                    transport.add(srcNode.nodeId);
                                    targetNode.transportTarget.put(blkid, transport);
                                } else {
                                    HashSet<String> transport = targetNode.transportTarget.get(blkid);
                                    transport.add(srcNode.nodeId);
                                }
                                targetNode.totalTransports++;
                                srcNode.totalTransports++;
                            }
                        }
                    }
                    LOG.info("!!!!!!!共分配了 " + Math.min(restTask, spareContainer) + " 个非本地性任务");
                }
                restTask = instance.applicationUnassignedTaskPool.get(appid).size();
                if (restTask < totalNodeContainer && restTask != 0) {
                    //尾部task
                    allocateRestTask();
                    break;
                }
                if (restTask == 0) break;
            }
            LOG.info("!!!Allocater end!!!");
            isStart = false;
        }
    }

    public void startContainerTaskAllocater(String appid, ArrayList<ContainerId> cids, String host) {
        ContainerTaskAllocater ca = new ContainerTaskAllocater(appid, cids, host);
        ca.start();
    }

    public class ContainerTaskAllocater extends Thread {
        public String appid;
        public boolean isStart;
        public ArrayList<ContainerId> cids;
        public String host;
        public ContainerTaskAllocater (String id, ArrayList<ContainerId> cid, String h) {
            appid = id;
            isStart = false;
            cids = cid;
            host = h;
        }
        @Override
        public void run() {
            LOG.info("=====allocateContainers: " + appid + "| cids:" + cids.size() + "|" + host);
            //给container分配task
            appid = formatAppId(appid);
            String nodeid = formatNodeId(host);
            if (!instance.applicationContainerTaskMap.containsKey(appid)) {
                instance.applicationContainerTaskMap.put(appid, new HashMap<String, String>());
            }
            HashMap<String, String> containertaskmap = instance.applicationContainerTaskMap.get(appid);
            //NEW
            int count = 0;
            HashSet<Task> tset = new HashSet<Task>();
            for (int i = 0; i < nodeTaskQueue.get(nodeid).size(); i++) {
                if (count == cids.size()) break;
                String cid = cids.get(count).toString();
                Task t = nodeTaskQueue.get(nodeid).get(i);
                if (t.appid.equals(appid)) {
                    if (t.status == TaskStatus.ALLOCATED) {
                        t.status = TaskStatus.COMPLETED;
                        containertaskmap.put(cid, t.no);
                        tset.add(t);
//                    LOG.info("!!!!!!! Node:" + nodeid + "|" + t.no + "->" + cid);
                        count++;
                        if (!t.locations.contains(host)) {
                            instance.applicationUnlocalityNumber.put(appid, instance.applicationUnlocalityNumber.get(appid) + 1);
                        }
                    }
                }
            }
            for (Task t : tset) {
                instance.applicationTaskFinalNode.get(appid).put(t.no, nodeid);
                nodeTaskQueue.get(nodeid).remove(t);
            }
            LOG.info("!!! Task->Container: " + containertaskmap.keySet().size());
        }
    }



    public String chooseDataNode(List<String> nodeids, String blockid, String hostname, String src, long offset, long length) {
        hostname = formatNodeId(hostname);
        if (nodeids.contains(hostname)) {
            return hostname;
        }
        String resultNode = null;
        String s = formatSplitSrc(src) + "|" + offset + "|" + length;
        LOG.info("!!! " + hostname + " chooseDataNode: " + s);
        String appid = null;
        for (Map.Entry<String, HashSet<String>> entry : instance.applicationSplitPool.entrySet()) {
            if (entry.getValue().contains(s)) {
                appid = entry.getKey();
                break;
            }
        }
        if (appid == null) {
            //中间文件
            for (String apid : instance.applicationChooseDNNumber.keySet()) {
                instance.applicationChooseDNNumber.put(apid, instance.applicationChooseDNNumber.get(apid) + 1);
                break;
            }
            int minTransport = Integer.MAX_VALUE;
            HashSet<String> loc = new HashSet<String>();
            for (String str : nodeids) {
                loc.add(str);
                Node node = nodeMap.get(str);
                if (node.totalTransports < minTransport) {
                    minTransport = node.totalTransports;
                    resultNode = str;
                }
            }
            if (nodeMap.containsKey(hostname)) {
                //排除集群以外的主机
                String blkid = getBlockLocationID(loc, hostname);
                blkid = blkid + "@" + resultNode;
                Transport ts = new Transport(blkid, blockid, hostname, resultNode);
                if (nodeMap.get(hostname).transportingBlock.containsKey(blockid)) {
                    LOG.info("!!!!!!!!ERROR: " + hostname + " 上存在重复的blockid " + blockid);
                }
                nodeMap.get(hostname).transportingBlock.put(blockid, ts);
                nodeMap.get(hostname).totalTransports++;
            }
            nodeMap.get(resultNode).totalTransports++;
        } else {
            instance.applicationChooseDNNumber.put(appid, instance.applicationChooseDNNumber.get(appid) + 1);
            Node node = nodeMap.get(hostname);
            HashSet<String> loc = new HashSet<String>();
            for (String str : nodeids) {
                loc.add(str);
            }
            String blkid = getBlockLocationID(loc, hostname);
            if (node.transportTarget.containsKey(blkid)) {
                for (String srcnodeid : node.transportTarget.get(blkid)) {
                    resultNode = srcnodeid;
                    break;
                }
                node.transportTarget.get(blkid).remove(blkid);
                if (node.transportTarget.get(blkid).size() == 0) {
                    node.transportTarget.remove(blkid);
                }
                blkid = blkid + "@" + resultNode;
                Transport ts = new Transport(blkid, blockid, hostname, resultNode);
                if (node.transportingBlock.containsKey(blockid)) {
                    LOG.info("!!!!!!!!ERROR: " + hostname + " 上存在重复的blockid " + blockid);
                }
                node.transportingBlock.put(blockid, ts);
            } else {
                //error
                LOG.info("!!!!!!!!ERROR: 在 " + hostname + " 上找不到blkid " + blkid + " 的记录");
                resultNode = nodeids.get(0);
            }
        }
        return resultNode;
    }

    public void blockLoadCompleted(String blokid, String nodeid, String src) {
        if (nodeid.equals(src)) return;
        Node node = nodeMap.get(nodeid);
        if (node == null) {
            //非集群主机的传输
            nodeMap.get(src).totalTransports--;
            LOG.info("!!!!! Block " + blokid + " 传输完成！"+ " from: " + src + " to" + nodeid);
            return;
        }
        if (node.transportingBlock.containsKey(blokid)) {
            LOG.info("!!!!! Block " + blokid + " 传输完成！"+ " from: " + src + " to" + nodeid);
            Transport ts = node.transportingBlock.get(blokid);
            node.totalTransports--;
            if (!ts.srcNode.equals(src)) {
                LOG.info("!!!!! Block " + blokid + " srcNode 不匹配！记录:" + ts.srcNode + "|实际:" + src);
                nodeMap.get(src).totalTransports--;
            } else {
                node.transportingBlock.remove(blokid);
                nodeMap.get(ts.srcNode).totalTransports--;
            }
        } else {
            LOG.info("!!!!! Block " + blokid + " from " + src + " to " + nodeid + " 找不到！");
        }
    }

    public String getBlockLocationID(HashSet<String> locations, String targetNodeId) {
        String[] loc = new String[locations.size()];
        int count = 0;
        for (String str : locations) {
            loc[count] = str;
            count++;
        }
        Arrays.sort(loc);
        StringBuilder sb = new StringBuilder();
        for (String str : loc) {
            if (sb.length() != 0) {
                sb.append("|");
            }
            sb.append(str);
        }
        return targetNodeId + "#" + sb.toString();
    }



    public HashMap<String, String> getTaskContainerMap(String appid, List<String> containerids) {
        appid = formatAppId(appid);
        HashMap<String, String> newmap = new HashMap<String, String>();
//        HashMap<String, Task> taskmap = instance.applicationTaskMap.get(appid);
        if (instance.applicationContainerTaskMap.containsKey(formatAppId(appid))) {
            HashMap<String, String> containertaskmap = instance.applicationContainerTaskMap.get(formatAppId(appid));
            for (String cid : containerids) {
                if (containertaskmap.containsKey(cid)) {
                    newmap.put(cid, containertaskmap.get(cid));
                    instance.applicationTaskStartTime.get(appid).put(containertaskmap.get(cid), System.currentTimeMillis());
                    LOG.info("!!!! cid = " + cid);
                } else {
                    LOG.info("!!ERROR 找不到 Container " + cid + " 对应的Task!!");
                }
            }
        }
        instance.applicationAMGetNumber.put(appid, instance.applicationAMGetNumber.get(appid) + 1);
        LOG.info("!!! getTaskContainerMap:发来 " + containerids.size() + " | 返回 " + newmap.entrySet().size());
        return newmap;
    }

    //统计block情况，初始位置加偏移量
    public void updateSplits(String appid, Map<String,String> src, Map<String,List<String>> locations, Map<String,Long> offset, Map<String,Long> length) {
        appid = formatAppId(appid);
        if (!instance.applicationSplitMap.containsKey(appid)) {
            instance.applicationSplitMap.put(appid, new HashMap<String, Split>());
        }
        if (!instance.applicationSplitPool.containsKey(appid)) {
            instance.applicationSplitPool.put(appid, new HashSet<String>());
        }
        HashMap<String, Split> splitMap = instance.applicationSplitMap.get(appid);
        HashSet<String> splitPool = instance.applicationSplitPool.get(appid);

            HashMap<String, ArrayList<Split>> sameFileSrc = new HashMap<String, ArrayList<Split>>();
        for (String spid : src.keySet()) {
            String s = src.get(spid);
            List<String> loc = locations.get(spid);
            HashSet<String> locset = new HashSet<String>();
            String locstr = "";
            for (String nodeid : loc) {
                locset.add(nodeid);
                locstr += loc + "|";
            }
            Long o = offset.get(spid);
            Long l = length.get(spid);
            s = formatSplitSrc(s);

            String id = s + "|" + o + "|" + l;
            Split sp = new Split(id, s, o, l, locset);
            if (!sameFileSrc.containsKey(s)) {
                sameFileSrc.put(s, new ArrayList<Split>());
            }
            sameFileSrc.get(s).add(sp);
            splitPool.add(id);
            splitMap.put(id, sp);
            LOG.info("!!!updateSplits: " + id + "|" + locstr);
        }
        Integer taski = 0;
        ArrayList<Split> jobSplits = new ArrayList<Split>();
        for (Map.Entry<String, ArrayList<Split>> entry : sameFileSrc.entrySet()) {
            if (entry.getValue().size() <= 1) {
                for (Split sp : entry.getValue()) {
                    sp.taskIndex = taski++;
                    jobSplits.add(sp);
                }
                continue;
            }
            ArrayList<Split> fileSplits = new ArrayList<Split>();
            Long lastOffset = 0L;
            for (int i = 0; i < entry.getValue().size(); i++) {
                Split selectSp = null;
                for (Split sp : entry.getValue()) {
                    if (sp.offset == lastOffset) {
                        selectSp = sp;
                        break;
                    }
                }
                lastOffset = selectSp.offset + selectSp.length;
                selectSp.taskIndex = taski++;
                fileSplits.add(selectSp);
            }
            Split lastSp = null;
            for (Split sp : fileSplits) {
                if (lastSp == null) {
                    lastSp = sp;
                    continue;
                }
                HashSet<String> re = new HashSet<String>();
                re.addAll(sp.location);
                re.retainAll(lastSp.location);
                if (re.size() != 0) {
                    //有交集
                    SplitChain spc = new SplitChain(fileSplits.get(0), lastSp, sp, re, fileSplits.indexOf(lastSp));
                    instance.applicationSplitChain.get(appid).add(spc);
                    LOG.info("SplitChain生成！！ start:" + spc.fileStartSplit.id + "| tar:" + spc.targetSplit.id + "| chain:" + spc.chainSplit.id);
                }
                lastSp = sp;
            }
        }
    }
}
