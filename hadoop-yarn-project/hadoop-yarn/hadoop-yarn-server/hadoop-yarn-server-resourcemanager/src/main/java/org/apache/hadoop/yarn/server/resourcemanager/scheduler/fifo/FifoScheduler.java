/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.major.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;

import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FifoScheduler extends
    AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode> implements
    Configurable {

  private static final Log LOG = LogFactory.getLog(FifoScheduler.class);
  private static final Log LOG2 = LogFactory.getLog("Major");
  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;

  private boolean usePortForNodeName;

  private ActiveUsersManager activeUsersManager;

  private static final String DEFAULT_QUEUE_NAME = "default";
  private QueueMetrics metrics;



//  private static final NetUsage usage = NetUsage.getInstance();
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  private final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public QueueMetrics getMetrics() {
      return metrics;
    }

    @Override
    public QueueInfo getQueueInfo( 
        boolean includeChildQueues, boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      queueInfo.setCapacity(1.0f);
      if (clusterResource.getMemory() == 0) {
        queueInfo.setCurrentCapacity(0.0f);
      } else {
        queueInfo.setCurrentCapacity((float) usedResource.getMemory()
            / clusterResource.getMemory());
      }
      queueInfo.setMaximumCapacity(1.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
      return queueInfo;
    }

    public Map<QueueACL, AccessControlList> getQueueAcls() {
      Map<QueueACL, AccessControlList> acls =
        new HashMap<QueueACL, AccessControlList>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(acl, new AccessControlList("*"));
      }
      return acls;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(
        UserGroupInformation unused) {
      QueueUserACLInfo queueUserAclInfo = 
        recordFactory.newRecordInstance(QueueUserACLInfo.class);
      queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
      queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
      return Collections.singletonList(queueUserAclInfo);
    }

    @Override
    public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
      return getQueueAcls().get(acl).isUserAllowed(user);
    }
    
    @Override
    public ActiveUsersManager getActiveUsersManager() {
      return activeUsersManager;
    }

    @Override
    public void recoverContainer(Resource clusterResource,
        SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return;
      }
      increaseUsedResources(rmContainer);
      updateAppHeadRoom(schedulerAttempt);
      updateAvailableResourcesMetrics();
    }
  };

  public FifoScheduler() {
    super(FifoScheduler.class.getName());
  }

  private synchronized void initScheduler(Configuration conf) {
    validateConf(conf);
    //Use ConcurrentSkipListMap because applications need to be ordered
    this.applications =
        new ConcurrentSkipListMap<ApplicationId, SchedulerApplication<FiCaSchedulerApp>>();
    this.minimumAllocation =
        Resources.createResource(conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    this.maximumAllocation =
        Resources.createResource(conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    this.usePortForNodeName = conf.getBoolean(
        YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
    this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false,
        conf);
    this.activeUsersManager = new ActiveUsersManager(metrics);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    initScheduler(conf);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }
  
  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public int getNumClusterNodes() {
    return nodes.size();
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public synchronized void
      reinitialize(Configuration conf, RMContext rmContext) throws IOException
  {
    setConf(conf);
  }

  @Override
  public Allocation allocate(
      ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
      List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {

    for (String appid : MajorServer.getInstance().getAppInfo().keySet()) {
      MajorAppInfo apif = MajorServer.getInstance().getAppInfo().get(appid);
      LOG.info("======MAJORSERVER AppInfo:" + appid + "|" + apif.toString());
    }

    for (String blockid : MajorServer.getInstance().getBlockChoices().keySet()) {
      List<String> nodeid = MajorServer.getInstance().getBlockChoices().get(blockid);
      StringBuilder sb = new StringBuilder();
      sb.append("======MAJORSERVER BlockChoices:");
      sb.append(blockid);
      for (String id: nodeid) {
        sb.append("|");
        sb.append(id);
      }
      LOG.info(sb.toString());
    }

    LOG.info("======MAJORSHI allocate");
    //LOG.info("======MAJORSHI allocate: ApplicationAttemptId:" + applicationAttemptId == null? "null" : applicationAttemptId.toString() + "| ask:" + ask == null? "null" :ask.size() + "| release:" + release == null? "null" :release.size() + "| blacklistAdditions:" + blacklistAdditions == null? "null" :blacklistAdditions.size() + "| blacklistRemovals:" + blacklistRemovals == null? "null" :blacklistRemovals.size());
    if (ask != null) {
      for (ResourceRequest rr : ask) {
        LOG2.info("======ASK: ResourceName:" + rr.getResourceName() + "| " + rr.toString());
      }
    }
    if (blacklistAdditions != null) {
      for (String s : blacklistAdditions) {
        LOG.info("======BlacklistAdditions: " + s);
      }
    }
    FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);

    if (application == null) {
      LOG.error("Calling allocate on removed " +
          "or non existant application " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    LOG.info("======MAJORSHI allocate:Sanity check");
    SchedulerUtils.normalizeRequests(ask, resourceCalculator,
            clusterResource, minimumAllocation, maximumAllocation);

    // Release containers
    LOG.info("======MAJORSHI allocate:Release containers start");
    for (ContainerId releasedContainer : release) {
      RMContainer rmContainer = getRMContainer(releasedContainer);
      if (rmContainer == null) {
         RMAuditLogger.logFailure(application.getUser(),
             AuditConstants.RELEASE_CONTAINER, 
             "Unauthorized access or invalid container", "FifoScheduler", 
             "Trying to release container not owned by app or with invalid id",
             application.getApplicationId(), releasedContainer);
      }
      containerCompleted(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              releasedContainer, 
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }
    LOG.info("======MAJORSHI allocate:Release containers end");
    MajorServer.getInstance().updateResourceRequest(ask, application.getApplicationId().toString());
    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " +
            "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) {
        LOG.debug("allocate: pre-update" +
            " applicationId=" + applicationAttemptId + 
            " application=" + application);
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        LOG.debug("allocate: post-update" +
            " applicationId=" + applicationAttemptId + 
            " application=" + application);
        application.showRequests();

        LOG.debug("allocate:" +
            " applicationId=" + applicationAttemptId + 
            " #ask=" + ask.size());
      }
      LOG.info("======MAJORSHI allocate:start update blacklist");
      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      LOG.info("======MAJORSHI allocate:pullNewlyAllocatedContainersAndNMTokens");
      ContainersAndNMTokensAllocation allocation =
          application.pullNewlyAllocatedContainersAndNMTokens();
      LOG.info("======MAJORSHI allocate:return");
      return new Allocation(allocation.getContainerList(),
        application.getHeadroom(), null, null, null,
        allocation.getNMTokenList());
    }
  }

  private FiCaSchedulerNode getNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }

  @VisibleForTesting
  public synchronized void addApplication(ApplicationId applicationId,
      String queue, String user) {

    SchedulerApplication<FiCaSchedulerApp> application =
        new SchedulerApplication<FiCaSchedulerApp>(DEFAULT_QUEUE, user);
    applications.put(applicationId, application);

    metrics.submitApp(user);
    LOG.info("Accepted application " + applicationId + " from user: " + user
            + ", currently num of applications: " + applications.size());
    rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
  }

  @VisibleForTesting
  public synchronized void
      addApplicationAttempt(ApplicationAttemptId appAttemptId,
          boolean transferStateFromPreviousAttempt,
          boolean shouldNotifyAttemptAdded) {
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(appAttemptId.getApplicationId());
    String user = application.getUser();
    // TODO: Fix store
    FiCaSchedulerApp schedulerApp =
        new FiCaSchedulerApp(appAttemptId, user, DEFAULT_QUEUE,
          activeUsersManager, this.rmContext);

    if (transferStateFromPreviousAttempt) {
      schedulerApp.transferStateFromPreviousAttempt(application
              .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(schedulerApp);

    metrics.submitAppAttempt(user);
    LOG.info("Added Application Attempt " + appAttemptId
            + " to scheduler from user " + application.getUser());
    if (shouldNotifyAttemptAdded) {
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptEvent(appAttemptId,
              RMAppAttemptEventType.ATTEMPT_ADDED));
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping notifying ATTEMPT_ADDED");
      }
    }
  }

  private synchronized void doneApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(applicationId);
    if (application == null){
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }
    MajorServer.getInstance().doneApplication(applicationId.toString());
    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(application.getUser(),
            applicationId);
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers)
      throws IOException {
    FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(applicationAttemptId.getApplicationId());
    if (application == null || attempt == null) {
      throw new IOException("Unknown application " + applicationAttemptId + 
      " has completed!");
    }

    // Kill all 'live' containers
    for (RMContainer container : attempt.getLiveContainers()) {
      if (keepContainers
          && container.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + container.getContainerId());
        continue;
      }
      containerCompleted(container,
        SchedulerUtils.createAbnormalContainerStatus(
          container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);
  }
  
  /**
   * Heart of the scheduler...
   * 
   * @param node node on which resources are available to be allocated
   */
  private void assignContainers(FiCaSchedulerNode node) {
    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " #applications=" + applications.size());
    // 这里应该询问Thrift服务器


    // Try to assign containers to applications in fifo order
    LOG.info("======MAJORSHI FifoScheuler.assignContainers():Try to assign containers to applications in fifo order. node = " + node != null ? node.toString() : "null");
    for (Map.Entry<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> e : applications
        .entrySet()) {
      FiCaSchedulerApp application = e.getValue().getCurrentAppAttempt();
      if (application == null) {
        continue;
      }

      LOG.debug("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }
        
        for (Priority priority : application.getPriorities()) {
//          LOG2.info("|| Proiority:" + priority.toString());
          int maxContainers = 
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.OFF_SWITCH); 
          // Ensure the application needs containers of this priority
          if (maxContainers > 0) {
            int assignedContainers = 0;
            if (MajorServer.getInstance().getApplicationStartDefualtAllocate(application.getApplicationId().toString())) {
              assignedContainers = assignContainersOnNode(node, application, priority);
            } else {
              ResourceRequest request =
                      application.getResourceRequest(priority, ResourceRequest.ANY);
              assignedContainers = assignContainerForMajor(node, application, priority, maxContainers, request, NodeType.OFF_SWITCH);
              if (assignedContainers == -1) {
                assignedContainers = assignContainersOnNode(node, application, priority);
              }
            }
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0) {
              break;
            }
          }
        }
      }
      
      LOG.debug("post-assignContainers");
      application.showRequests();

      // Done
      if (Resources.lessThan(resourceCalculator, clusterResource,
              node.getAvailableResource(), minimumAllocation)) {
        break;
      }
    }

    // Update the applications' headroom to correctly take into
    // account the containers assigned in this update.
    for (SchedulerApplication<FiCaSchedulerApp> application : applications.values()) {
      FiCaSchedulerApp attempt =
          (FiCaSchedulerApp) application.getCurrentAppAttempt();
      if (attempt == null) {
        continue;
      }
      updateAppHeadRoom(attempt);
    }
  }

  private int getMaxAllocatableContainers(FiCaSchedulerApp application,
      Priority priority, FiCaSchedulerNode node, NodeType type) {
//    LOG2.info("|||" + node.getNodeName() + " getMaxContainer start");
    int maxContainers = 0;
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchRequest != null) {
      maxContainers = offSwitchRequest.getNumContainers();
    }
//    LOG2.info("|||getMaxAllocatableContainers 1:" + node.getNodeName() + "|" + type.toString() + "|" + maxContainers);
    if (type == NodeType.OFF_SWITCH) {
//      LOG2.info("|||" + node.getNodeName() + " getMaxContainer end 1");
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = 
        application.getResourceRequest(priority, node.getRMNode().getRackName());
//      LOG2.info("|||getMaxAllocatableContainers 2:" + node.getNodeName() + "|" + type.toString() + "|" + maxContainers);
      if (rackLocalRequest == null) {
//        LOG2.info("|||" + node.getNodeName() + " getMaxContainer end 2");
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
      if (nodeLocalRequest != null) {
        maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
      }
//      LOG2.info("|||getMaxAllocatableContainers 3:"+ node.getNodeName() + "|" + type.toString() + "|" + maxContainers);
    }
//    LOG2.info("|||" + node.getNodeName() + " getMaxContainer end 3");
    return maxContainers;
  }


  private int assignContainersOnNode(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority 
  ) {
    LOG2.info("|||assignContainersOnNode " + node.getNodeName() + " start");
    // Data-local
    int nodeLocalContainers = 
      assignNodeLocalContainers(node, application, priority); 

    // Rack-local
    int rackLocalContainers = 
      assignRackLocalContainers(node, application, priority);

    // Off-switch
    int offSwitchContainers =
            assignOffSwitchContainers(node, application, priority);
    LOG.debug("assignContainersOnNode:" +
            " node=" + node.getRMNode().getNodeAddress() +
            " application=" + application.getApplicationId().getId() +
            " priority=" + priority.getPriority() +
            " #assigned=" +
            (nodeLocalContainers + rackLocalContainers + offSwitchContainers));
    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }

  private int assignNodeLocalContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getNodeName());
    if (request != null) {
      // Don't allocate on this node if we don't need containers on this rack
      ResourceRequest rackRequest =
          application.getResourceRequest(priority, 
              node.getRMNode().getRackName());
      if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
        return 0;
      }
      
      int assignableContainers =
              Math.min(
                      getMaxAllocatableContainers(application, priority, node,
                              NodeType.NODE_LOCAL),
                      request.getNumContainers());
//                request.getNumContainers();
      assignedContainers = 
        assignContainer(node, application, priority, 
            assignableContainers, request, NodeType.NODE_LOCAL);
    }
    return assignedContainers;
  }

  private int assignRackLocalContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRMNode().getRackName());
    if (request != null) {
      // Don't allocate on this rack if the application doens't need containers
      ResourceRequest offSwitchRequest =
          application.getResourceRequest(priority, ResourceRequest.ANY);
      if (offSwitchRequest.getNumContainers() <= 0) {
        return 0;
      }
      
      int assignableContainers =
              Math.min(
                      getMaxAllocatableContainers(application, priority, node,
                              NodeType.RACK_LOCAL),
                      request.getNumContainers());
//                request.getNumContainers();
//      LOG2.info("---assignableContainers:" + assignableContainers);
      assignedContainers = 
        assignContainer(node, application, priority, 
            assignableContainers, request, NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, ResourceRequest.ANY);
    if (request != null) {
      assignedContainers = 
        assignContainer(node, application, priority, 
            request.getNumContainers(), request, NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainerForMajor(FiCaSchedulerNode node, FiCaSchedulerApp application,
                              Priority priority, int assignableContainers,
                              ResourceRequest request, NodeType type) {
    LOG.debug("assignContainers:" +
            " node=" + node.getRMNode().getNodeAddress() +
            " application=" + application.getApplicationId().getId() +
            " priority=" + priority.getPriority() +
            " assignableContainers=" + assignableContainers +
            " request=" + request + " type=" + type);
    Resource capability = request.getCapability();

    int availableContainers =
            node.getAvailableResource().getMemory() / capability.getMemory(); // TODO: A buggy
    // application
    // with this
    // zero would
    // crash the
    // scheduler.
    int assignedContainers =
            Math.min(assignableContainers, availableContainers);
    if (request.getPriority().getPriority() == 20) {
      assignedContainers = MajorServer.getInstance().getAllocatedContainerNumber(node.getNodeName(), application.getApplicationId().toString(), request.getCapability().getMemory(), node.getAvailableResource().getMemory(), assignableContainers);
      if (assignedContainers == -1) return -1;
    } else {
      LOG2.info("--- priority:" + request.getPriority().getPriority() + "| assign " + assignedContainers + " containers");
    }
//    LOG2.info("-----在 " + node.getNodeName() + "上需求 " + assignableContainers + " 个Container, 可申请 " + availableContainers + " 个, 资源名为" + request.getResourceName() + ", 最终结果为 " + assignedContainers);
//    LOG2.info("-----assigned final: " + assignedContainers);
//    assignedContainers = MajorServer.getInstance().getAllocatedContainerNumber(node.getNodeName(), application.getApplicationId().toString(), availableContainers);
    //分配的数量
//    LOG.info("======MAJORSHI assignContainer: assignedContainers=" + assignedContainers);

    ArrayList<ContainerId> cids = new ArrayList<ContainerId>();

    if (assignedContainers > 0) {
      for (int i = 0; i < assignedContainers; ++i) {
        NodeId nodeId = node.getRMNode().getNodeID();
        ContainerId containerId = BuilderUtils.newContainerId(application
                .getApplicationAttemptId(), application.getNewContainerId());
        cids.add(containerId);
        // Create the container
        LOG.info("======MAJORSHI assignContainer:newContainer()");
        Container container =
                BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
                        .getHttpAddress(), capability, priority, null);
        if (container != null) {
          LOG.info("======MAJORSHI assignContainer: " + container.toString());
        }
        // Allocate!

        // Inform the application
        LOG.info("======MAJORSHI assignContainer:application.allocate()");
        RMContainer rmContainer =
                application.allocate(type, node, priority, request, container);
        // Inform the node
        node.allocateContainer(rmContainer);
        // Update usage for this container
        increaseUsedResources(rmContainer);
      }
    }
    if (request.getPriority().getPriority() == 20) {
      MajorServer.getInstance().reportAllocatedContainerNumber(node.getNodeName(), application.getApplicationId().toString(), availableContainers);
      MajorServer.getInstance().startContainerTaskAllocater(application.getApplicationId().toString(), cids, node.getNodeName());
//      MajorServer.getInstance().allocateContainers(application.getApplicationId().toString(), cids, node.getNodeName());
    }
    return assignedContainers;
  }

  private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, 
      Priority priority, int assignableContainers, 
      ResourceRequest request, NodeType type) {

    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " application=" + application.getApplicationId().getId() + 
        " priority=" + priority.getPriority() + 
        " assignableContainers=" + assignableContainers +
        " request=" + request + " type=" + type);
    Resource capability = request.getCapability();

    int availableContainers = 
      node.getAvailableResource().getMemory() / capability.getMemory(); // TODO: A buggy
                                                                        // application
                                                                        // with this
                                                                        // zero would
                                                                        // crash the
                                                                        // scheduler.
    int assignedContainers =
            Math.min(assignableContainers, availableContainers);
    LOG2.info("-----assignedContainers:" + node.getNodeName() + "|" + assignableContainers + "|" + availableContainers + "|" + request.getResourceName() + "|" + request.getNumContainers());
    LOG2.info("-----assigned final: " + assignedContainers);
//    assignedContainers = MajorServer.getInstance().getAllocatedContainerNumber(node.getNodeName(), application.getApplicationId().toString(), availableContainers);
    //分配的数量
//    LOG.info("======MAJORSHI assignContainer: assignedContainers=" + assignedContainers);
    if (assignedContainers > 0) {
      for (int i=0; i < assignedContainers; ++i) {
        NodeId nodeId = node.getRMNode().getNodeID();
        ContainerId containerId = BuilderUtils.newContainerId(application
                .getApplicationAttemptId(), application.getNewContainerId());

        // Create the container
        LOG.info("======MAJORSHI assignContainer:newContainer()");
        Container container =
            BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
              .getHttpAddress(), capability, priority, null);
        if (container != null) {
          LOG.info("======MAJORSHI assignContainer: " + container.toString());
        }
        // Allocate!
        
        // Inform the application
        LOG.info("======MAJORSHI assignContainer:application.allocate()");
        RMContainer rmContainer =
            application.allocate(type, node, priority, request, container);
        
        // Inform the node
        node.allocateContainer(rmContainer);

        // Update usage for this container
        increaseUsedResources(rmContainer);
      }
      MajorServer.getInstance().reportAllocatedContainerNumber(node.getNodeName(), application.getApplicationId().toString(), availableContainers);
    }
    
    return assignedContainers;
  }

  private synchronized void nodeUpdate(RMNode rmNode) {
    FiCaSchedulerNode node = getNode(rmNode.getNodeID());
    //Major report
    MajorServer.getInstance().updateNodeAvaliableMemory(node.getNodeName(), node.getAvailableResource().getMemory(), node.getAvailableResource().getVirtualCores(), node.getTotalResource().getMemory(), node.getTotalResource().getVirtualCores());

    // Update resource if any change
    SchedulerUtils.updateResourceIfChanged(node, rmNode, clusterResource, LOG);
    
    List<UpdatedContainerInfo> containerInfoList = rmNode.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      containerCompleted(getRMContainer(containerId), 
          completedContainer, RMContainerEventType.FINISHED);
    }

    if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
            node.getAvailableResource(),minimumAllocation)) {
      LOG.debug("Node heartbeat " + rmNode.getNodeID() + 
          " available resource = " + node.getAvailableResource());

      assignContainers(node);

      LOG.debug("Node after allocation " + rmNode.getNodeID() + " resource = "
          + node.getAvailableResource());
    }

    updateAvailableResourcesMetrics();
  }

  private void increaseUsedResources(RMContainer rmContainer) {
    Resources.addTo(usedResource, rmContainer.getAllocatedResource());
  }

  private void updateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt) {
    schedulerAttempt.setHeadroom(Resources.subtract(clusterResource,
      usedResource));
  }

  private void updateAvailableResourcesMetrics() {
    metrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource,
      usedResource));
  }

  @Override
  public void handle(SchedulerEvent event) {
    LOG.info("======MAJORSHI handle:" + event.getType().toString());
    switch(event.getType()) {
    case NODE_ADDED:
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
        nodeAddedEvent.getAddedRMNode());

    }
    break;
    case NODE_REMOVED:
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    case NODE_UPDATE:
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = 
      (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode());
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      LOG.info("======MAJORSHI APP_ADDED: " + appAddedEvent.getQueue());
      addApplication(appAddedEvent.getApplicationId(),
        appAddedEvent.getQueue(), appAddedEvent.getUser());
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED:
    {
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getShouldNotifyAttemptAdded());
    }
    break;
    case APP_ATTEMPT_REMOVED:
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      try {
        doneApplicationAttempt(
          appAttemptRemovedEvent.getApplicationAttemptID(),
          appAttemptRemovedEvent.getFinalAttemptState(),
          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
      } catch(IOException ie) {
        LOG.error("Unable to remove application "
            + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
      }
    }
    break;
    case CONTAINER_EXPIRED:
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent = 
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerid = containerExpiredEvent.getContainerId();
      containerCompleted(getRMContainer(containerid), 
          SchedulerUtils.createAbnormalContainerStatus(
              containerid, 
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private void containerLaunchedOnNode(ContainerId containerId, FiCaSchedulerNode node) {
    // Get the application for the finished container
    FiCaSchedulerApp application = getCurrentAttemptForContainer(containerId);
    if (application == null) {
      LOG.info("Unknown application "
          + containerId.getApplicationAttemptId().getApplicationId()
          + " launched container " + containerId + " on node: " + node);
      // Some unknown container sneaked into the system. Kill it.
      this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));

      return;
    }
    
    application.containerLaunchedOnNode(containerId, node.getNodeID());
  }

  @Lock(FifoScheduler.class)
  private synchronized void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }

    // Get the application for the finished container
    Container container = rmContainer.getContainer();
    FiCaSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    
    // Get the node on which the container was allocated
    FiCaSchedulerNode node = getNode(container.getNodeId());
    
    if (application == null) {
      LOG.info("Unknown application: " + appId + 
          " released container " + container.getId() +
          " on node: " + node + 
          " with event: " + event);
      return;
    }

    // Inform the application
    application.containerCompleted(rmContainer, containerStatus, event);

    // Inform the node
    node.releaseContainer(container);
    
    // Update total usage
    Resources.subtractFrom(usedResource, container.getResource());

    LOG.info("Application attempt " + application.getApplicationAttemptId() + 
        " released container " + container.getId() +
        " on node: " + node + 
        " with event: " + event);
     
  }
  
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  private synchronized void removeNode(RMNode nodeInfo) {
    FiCaSchedulerNode node = getNode(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
    // Kill running containers
    for(RMContainer container : node.getRunningContainers()) {
      containerCompleted(container, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER),
              RMContainerEventType.KILL);
    }
    
    //Remove the node
    this.nodes.remove(nodeInfo.getNodeID());
    
    // Update cluster metrics
    Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
  }

  @Override
  public QueueInfo getQueueInfo(String queueName,
      boolean includeChildQueues, boolean recursive) {
    return DEFAULT_QUEUE.getQueueInfo(false, false);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return DEFAULT_QUEUE.getQueueUserAclInfo(null); 
  }

  private synchronized void addNode(RMNode nodeManager) {
    this.nodes.put(nodeManager.getNodeID(), new FiCaSchedulerNode(nodeManager,
        usePortForNodeName));
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
  }

  @Override
  public void recover(RMState state) {
    // NOT IMPLEMENTED
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    FiCaSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return DEFAULT_QUEUE.getMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    return DEFAULT_QUEUE.hasAccess(acl, callerUGI);
  }

  @Override
  public synchronized List<ApplicationAttemptId>
      getAppsInQueue(String queueName) {
    if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
      List<ApplicationAttemptId> attempts =
          new ArrayList<ApplicationAttemptId>(applications.size());
      for (SchedulerApplication<FiCaSchedulerApp> app : applications.values()) {
        attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
      }
      return attempts;
    } else {
      return null;
    }
  }

  public Resource getUsedResource() {
    return usedResource;
  }
}
