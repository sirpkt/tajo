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

package org.apache.tajo.master.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.QueryInProgress;
import org.apache.tajo.master.QueryManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * tajo-multiple-queue.xml
 *
 */
public class FairScheduler implements Scheduler {
  private static final Log LOG = LogFactory.getLog(FairScheduler.class.getName());

  public static final String QUEUE_KEY_REPFIX = "tajo.fair.queue";
  public static final String QUEUE_NAMES_KEY = QUEUE_KEY_REPFIX + ".names";

  private Map<String, QueueProperty> queueProperties = new HashMap<String, QueueProperty>();
  private Map<String, LinkedList<QuerySchedulingInfo>> queues = new HashMap<String, LinkedList<QuerySchedulingInfo>>();
  private Map<QueryId, String> queryAssignedMap = Maps.newConcurrentMap();
  private final Thread queryProcessor;
  private AtomicBoolean stopped = new AtomicBoolean();
  private QueryManager manager;

  public FairScheduler(QueryManager manager) {
    this.manager = manager;
    this.queryProcessor = new Thread(new FairQueryProcessor());
    this.queryProcessor.setName("Fair Query Processor");
    initQueue();
  }

  private void reorganizeQueue(List<QueueProperty> newQueryList) {
    Set<String> previousQueueNames = new HashSet<String>(queues.keySet());

    for (QueueProperty eachQueue: newQueryList) {
      queueProperties.put(eachQueue.getQueueName(), eachQueue);
      if (!previousQueueNames.remove(eachQueue.getQueueName())) {
        // not existed queue
        LinkedList<QuerySchedulingInfo> queue = new LinkedList<QuerySchedulingInfo>();
        queues.put(eachQueue.getQueueName(), queue);
        LOG.info("Queue [" + eachQueue + "] added");
      }
    }

    // Removed queue
    for (String eachRemovedQueue: previousQueueNames) {
      queueProperties.remove(eachRemovedQueue);

      LinkedList<QuerySchedulingInfo> queue = queues.remove(eachRemovedQueue);
      LOG.info("Queue [" + eachRemovedQueue + "] removed");
      if (queue != null) {
        for (QuerySchedulingInfo eachQuery: queue) {
          LOG.warn("Remove waiting query: " + eachQuery + " from " + eachRemovedQueue + " queue");
        }
      }

      queue.clear();
    }
  }

  private void initQueue() {
    List<QueueProperty> queueList = loadQueueProperty(manager.getMasterContext().getConf());

    synchronized (queues) {
      if (!queues.isEmpty()) {
        reorganizeQueue(queueList);
        return;
      }

      for (QueueProperty eachQueue : queueList) {
        LinkedList<QuerySchedulingInfo> queue = new LinkedList<QuerySchedulingInfo>();
        queues.put(eachQueue.getQueueName(), queue);
        queueProperties.put(eachQueue.getQueueName(), eachQueue);
        LOG.info("Queue [" + eachQueue + "] added");
      }
    }
  }

  public static List<QueueProperty> loadQueueProperty(TajoConf tajoConf) {
    TajoConf queueConf = new TajoConf(tajoConf);
    queueConf.addResource("tajo-fair-queue.xml");

    List<QueueProperty> queueList = new ArrayList<QueueProperty>();

    String queueNameProperty = queueConf.get(QUEUE_NAMES_KEY);
    if (queueNameProperty == null || queueNameProperty.isEmpty()) {
      LOG.warn("Can't found queue. added " + QueueProperty.DEFAULT_QUEUE_NAME + " queue");
      QueueProperty queueProperty = new QueueProperty(QueueProperty.DEFAULT_QUEUE_NAME, 1, -1);
      queueList.add(queueProperty);

      return queueList;
    }

    String[] queueNames = queueNameProperty.split(",");
    for (String eachQueue: queueNames) {
      String maxPropertyKey = QUEUE_KEY_REPFIX + "." + eachQueue + ".max";
      String minPropertyKey = QUEUE_KEY_REPFIX + "." + eachQueue + ".min";

      if (StringUtils.isEmpty(queueConf.get(maxPropertyKey)) || StringUtils.isEmpty(queueConf.get(minPropertyKey))) {
        LOG.error("Can't find " + maxPropertyKey + " or "+ minPropertyKey + " in tajo-fair-queue.xml");
        continue;
      }

      QueueProperty queueProperty = new QueueProperty(eachQueue,
          queueConf.getInt(minPropertyKey, 1),
          queueConf.getInt(maxPropertyKey, 1));
      queueList.add(queueProperty);
    }

    return queueList;
  }

  @Override
  public void start() {
    queryProcessor.start();

  }

  @Override
  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }
    synchronized (queryProcessor) {
      queryProcessor.interrupt();
    }
  }

  @Override
  public Mode getMode() {
    return Mode.FAIR;
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public boolean addQuery(QueryInProgress queryInProgress) {
    QuerySchedulingInfo querySchedulingInfo =
        new QuerySchedulingInfo(queryInProgress.getQueryId(), 1, queryInProgress.getQueryInfo().getStartTime());

    boolean added = addQueryToQueue(querySchedulingInfo, queryInProgress.getQueryInfo().getQueryContext());
    wakeupProcessor();

    return added;
  }

  protected void wakeupProcessor() {
    synchronized (queryProcessor) {
      queryProcessor.notifyAll();
    }
  }

  public List<QueryInProgress> getRunningQueries(String queueName) {
    ArrayList<QueryInProgress> runningQueries = new ArrayList<QueryInProgress>();
    for (QueryInProgress eachQuery : manager.getRunningQueries()) {
      if(queryAssignedMap.get(eachQuery.getQueryId()).equals(queueName)){
        runningQueries.add(eachQuery);
      }
    }
    return runningQueries;
  }

  @Override
  public boolean removeQuery(QueryId queryId) {
    return notifyQueryStop(queryId);
  }

  public boolean notifyQueryStop(QueryId queryId) {
    synchronized (queues) {
      String queueName = queryAssignedMap.remove(queryId);


      LinkedList<QuerySchedulingInfo> queue = queues.get(queueName);
      if (queue == null) {
        LOG.error("Can't get queue from multiple queue: " + queryId.toString() + ", queue=" + queueName);
        return false;
      }

      // If the query is a waiting query, remove from a queue.
      LOG.info(queryId.toString() + " is not a running query. Removing from queue.");
      QuerySchedulingInfo stoppedQuery = null;
      for (QuerySchedulingInfo eachQuery: queue) {
        if (eachQuery.getQueryId().equals(queryId)) {
          stoppedQuery = eachQuery;
          break;
        }
      }

      if (stoppedQuery != null) {
        queue.remove(stoppedQuery);
      } else {
        LOG.error("No query info in the queue: " + queryId + ", queue=" + queueName);
        return false;
      }
    }
    wakeupProcessor();
    return true;
  }

  protected QuerySchedulingInfo[] getScheduledQueries() {
    synchronized (queues) {
      List<QuerySchedulingInfo> queries = new ArrayList<QuerySchedulingInfo>();

      for (String eachQueueName : queues.keySet()) {
        int runningSize = getRunningQueries(eachQueueName).size();
        QueueProperty property = queueProperties.get(eachQueueName);

        if(property.getMaxCapacity() == -1){
          LinkedList<QuerySchedulingInfo> queue = queues.get(eachQueueName);
          if (queue != null && queue.size() > 0) {
            QuerySchedulingInfo querySchedulingInfo = queue.pollFirst();
            queries.add(querySchedulingInfo);
            try {
              manager.startQueryJob(querySchedulingInfo.getQueryId());
            } catch (Exception e) {
              LOG.fatal("Exception during query startup:", e);
              manager.stopQuery(querySchedulingInfo.getQueryId());
            }
          }
        } else {
          if (property.getMinCapacity() * (runningSize + 1) < property.getMaxCapacity()) {
            LinkedList<QuerySchedulingInfo> queue = queues.get(eachQueueName);
            if (queue != null && queue.size() > 0) {
              QuerySchedulingInfo querySchedulingInfo = queue.pollFirst();
              queries.add(querySchedulingInfo);
              try {
                manager.startQueryJob(querySchedulingInfo.getQueryId());
              } catch (Exception e) {
                LOG.fatal("Exception during query startup:", e);
                manager.stopQuery(querySchedulingInfo.getQueryId());
              }
            } else {
              continue;
            }
          }
        }
      }

      return queries.toArray(new QuerySchedulingInfo[]{});
    }
  }

  protected boolean addQueryToQueue(QuerySchedulingInfo querySchedulingInfo, QueryContext queryContext) {
    String submitQueueNameProperty = queryContext.get(ConfVars.JOB_QUEUE_NAMES.varname,
        ConfVars.JOB_QUEUE_NAMES.defaultVal);

    String queueName = submitQueueNameProperty.split(",")[0];
    synchronized (queues) {
      LinkedList<QuerySchedulingInfo> queue = queues.get(queueName);

      if (queue != null) {

        querySchedulingInfo.setAssignedQueueName(queueName);
        queryContext.set(QueueProperty.QUERY_QUEUE_KEY, queueName);
        querySchedulingInfo.setCandidateQueueNames(Sets.newHashSet(queueName));
        queue.push(querySchedulingInfo);

        queryAssignedMap.put(querySchedulingInfo.getQueryId(), queueName);

        LOG.info(querySchedulingInfo.getQueryId() + " is assigned to the [" + queueName + "] queue");

        return true;
      } else {
        LOG.error("Can't find proper query queue(requested queue=" + submitQueueNameProperty + ")");

        return false;
      }
    }
  }

  @Override
  public String getStatusHtml() {
    StringBuilder sb = new StringBuilder();

    String runningQueryList = "";
    String waitingQueryList = "";

    String prefix = "";

    sb.append("<table border=\"1\" width=\"100%\" class=\"border_table\">");
    sb.append("<tr><th width='200'>Queue</th><th width='100'>Min Slot</th><th width='100'>Max Slot</th><th>Running Query</th><th>Waiting Queries</th></tr>");

    synchronized (queues) {
      SortedSet<String> queueNames = new TreeSet<String>(queues.keySet());
      for (String eachQueueName : queueNames) {
        waitingQueryList = "";
        runningQueryList = "";

        QueueProperty queryProperty = queueProperties.get(eachQueueName);
        sb.append("<tr>");
        sb.append("<td>").append(eachQueueName).append("</td>");

        sb.append("<td align='right'>").append(queryProperty.getMinCapacity()).append("</td>");
        sb.append("<td align='right'>").append(queryProperty.getMaxCapacity()).append("</td>");

        for (QueryInProgress eachQuery : manager.getRunningQueries()) {
          if (eachQueueName.equals(queryAssignedMap.get(eachQuery.getQueryId()))) {
            runningQueryList += prefix + eachQuery.getQueryId() + ",";
          }
        }

        prefix = "";
        for (QuerySchedulingInfo eachQuery : queues.get(eachQueueName)) {
          waitingQueryList += prefix + eachQuery.getQueryId() +
              "<input id=\"btnSubmit\" type=\"submit\" value=\"Remove\" onClick=\"javascript:killQuery('" + eachQuery.getQueryId() + "');\">";
          prefix = "<br/>";
        }

        sb.append("<td>").append(runningQueryList).append("</td>");
        sb.append("<td>").append(waitingQueryList).append("</td>");
        sb.append("</tr>");
      }
    }

    sb.append("</table>");
    return sb.toString();
  }

  private class PropertyReloader extends Thread {
    public PropertyReloader() {
      super("FairScheduler-PropertyReloader");
    }

    @Override
    public void run() {
      LOG.info("FairScheduler-PropertyReloader started");
      while (!stopped.get()) {
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
        initQueue();
      }
    }
  }

  private final class FairQueryProcessor implements Runnable {
    @Override
    public void run() {

      QuerySchedulingInfo[] queries;

      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        queries = getScheduledQueries();

        if (queries != null && queries.length > 0) {
          for (QuerySchedulingInfo eachQuery : queries) {
            try {
              manager.startQueryJob(eachQuery.getQueryId());
            } catch (Throwable t) {
              LOG.error("Exception during query startup:", t);
              manager.stopQuery(eachQuery.getQueryId());
            }
          }
        }

        synchronized (queryProcessor) {
          try {
            queryProcessor.wait(500);
          } catch (InterruptedException e) {
            if (stopped.get()) {
              break;
            }
            LOG.warn("Exception during shutdown: ", e);
          }
        }
      }
    }
  }

}