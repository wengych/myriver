/*
 * 
 */
package org.elasticsearch.river.my;

import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

/**
 *
 * @author weng
 */
public class MyRiver extends AbstractRiverComponent implements River{

  private final Client client;
  
  private volatile boolean closed;
  
  private volatile Thread thread;
  
  private TaskManager taskManager;
  
  @SuppressWarnings({"unchecked"})
  @Inject
  public MyRiver(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);
    this.client = client;
    
    if (settings.settings().containsKey("my")) {
      logger.info("Creating my river.");
      Map<String, Object> mySettings = (Map<String, Object>) settings.settings().get("my");
      
      Object obj = mySettings.get("updateRate");
      long updateRate = 10000;
      if (obj != null)
        Integer.parseInt(obj.toString());
      taskManager = new TaskManager(this, client, updateRate, logger);
    }
  }
  
  @Override
  public void start() {
    logger.info("starting my river.");
    
    try {
      client.admin().indices().prepareCreate(riverName.name()).execute().actionGet();
    } catch (IndexAlreadyExistsException e) {
      // That's fine.
      logger.info("Index exists.", riverName.name());
    } catch (ClusterBlockException e) {
      // Do nothing, seems somehthing wrong with it.
    } catch (Exception e) {
      logger.warn("failed to create index {[]}, disabling river", e, riverName.name());
    }
    
    thread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
            "task_manager").newThread(taskManager);
    
    thread.start();
    logger.info("Created thread for Task Manager.");
  }

  @Override
  public void close() {
    logger.info("close my river.");
    
    if (this.thread != null) {
      this.thread.interrupt();
    }
    
    /*try {
      client.admin().indices().prepareDelete(riverName.name()).execute().actionGet();
    } catch (Exception e) {
      logger.warn("failed to delete index {[]}.", e, riverName.name());
    }*/
    
    closed = true;
  }
  
}
