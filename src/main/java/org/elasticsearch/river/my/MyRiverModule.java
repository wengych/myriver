/*
 *
 */
package org.elasticsearch.river.my;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 * @author weng
 */
public class MyRiverModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(River.class).to(MyRiver.class).asEagerSingleton();
  }
  
}
