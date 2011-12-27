/*
 * 
 */
package org.elasticsearch.river.my;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
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
  
  @SuppressWarnings({"unchecked"})
  @Inject
  public MyRiver(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);
    this.client = client;
    
    logger.info("Creating my river.");
  }
  
  @Override
  public void start() {
    logger.info("starting my river.");
    // throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void close() {
    logger.info("close my river.");
    // throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
