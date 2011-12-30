/*
 *
 */
package org.elasticsearch.plugin.river.my;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.my.MyRiverModule;

public class MyRiverPlugin extends AbstractPlugin {

  @Inject
  public MyRiverPlugin() {

  }

  @Override
  public String name() {
    return "river-my";
  }

  @Override
  public String description() {
    return "River My Plugin";
  }
  
  @Override
  public void processModule(Module module) {
    if (module instanceof RiversModule) {
      ((RiversModule) module).registerRiver("my", MyRiverModule.class);
    }
  }
}
