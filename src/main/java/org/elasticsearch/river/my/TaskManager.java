/*
 *
 */
package org.elasticsearch.river.my;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.search.SearchHit;

/**
 *
 * @author weng
 */
public class TaskManager implements Runnable {

  private Client client;
  private ESLogger logger;
  private long updateRate;
  private AbstractRiverComponent river;
  private Map<String, Task> taskMap;
  private ArrayList taskArr;
  private int currentTaskIndex;
  private Task currentTask;

  private void InitTasks() throws Exception {
    QueryBuilder builder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_id", "_meta"));

    SearchRequestBuilder search = client.prepareSearch("_river");
    search.setTypes(river.riverName().name());
    search.setQuery(builder);
    SearchResponse resp = search.execute().actionGet();

    int hitCount = 0;
    for (SearchHit hit : resp.hits().getHits()) {
      logger.info("Task Manager: Query response hits[ " + Integer.toString(hitCount) + "]: " + hit.sourceAsString());
      hitCount++;

      Map<String, Object> sourceMap = hit.sourceAsMap();
      Map<String, Object> my = (Map<String, Object>) sourceMap.get("my");
      ArrayList arr = (ArrayList)my.get("tasks");
      for (Object taskObj: arr) {
        Task newTask = new Task((Map<String, String>)taskObj, client, river.riverName().name());
        taskArr.add(newTask);
        taskMap.put(newTask.id(), newTask);
      }
    }
    
    currentTaskIndex = 0;
    currentTask = (Task) taskArr.get(currentTaskIndex);
  }
  
  private Task GetNextTask() {
    currentTaskIndex++;
    if (currentTaskIndex >= taskArr.size())
      return null;
    
    currentTask = (Task) taskArr.get(currentTaskIndex);
    return currentTask;
  }

  public TaskManager(MyRiver river, Client client, long updateRate, ESLogger logger) {
    this.river = river;
    this.client = client;
    this.logger = logger;
    this.updateRate = updateRate;
    this.currentTaskIndex = 0;
    
    this.taskArr = new ArrayList();
    this.taskMap = new HashMap();

    try {
      InitTasks();
    } catch (Exception e) {
      logger.info("Task Manager: error.");
    }
  }

  @Override
  public void run() {
    if (logger.isDebugEnabled()) {
      logger.debug("Create task manager thread.");
    }

    do {
      logger.info("TaskManager: current task index: "
              + Integer.toString(currentTaskIndex));
      try {
        String output = currentTask.Run();
        logger.info("Task {[]} output: {[]}", currentTask.id(), output);
        logger.info("Task [" +  currentTask.id() + "] output: " + output);
      } catch (IOException ex) {
        logger.error("TaskManager: IOException");
      } catch (InterruptedException ex) {
        logger.error("TaskManager: Interrupted Exception");
      }
      
      currentTask = GetNextTask();
    } while (null != currentTask);
    
    DeleteMappingRequest req = new DeleteMappingRequest("_river");
    req.type(river.riverName().name());
    DeleteMappingResponse resp = client.admin().indices().deleteMapping(req).actionGet();
    logger.info("TaskManager: delete request: " + resp.toString());
  }
}
