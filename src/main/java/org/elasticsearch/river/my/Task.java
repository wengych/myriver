/*
 *
 */
package org.elasticsearch.river.my;

import org.elasticsearch.action.index.IndexResponse;
import java.util.Date;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 *
 * @author weng
 */
public class Task {
  /*
  {
  "type": "my",
  "my": {
  "conf": {
  "customer": "vfuk",
  "db": {"server": "localhost", "name": "mis_db", "user": "weng", "password": ""}
  },
  "tasks": [
  {
  "id": "ls",
  "cmd": "ls",
  "priority": "high",
  "status": "waiting"
  }
  ]
  }
  }
   */

  private String id;
  private String cmd;
  private String priority;
  private String status;
  private Process process;
  private Client client;
  private String indexName;

  public String id() {
    return id;
  }
  
  private void WriteRunningStatus(String line) throws IOException {
    String currentTime = Long.toString(new Date().getTime());
    IndexResponse resp = client.prepareIndex(indexName, id, currentTime)
            .setSource(jsonBuilder()
              .startObject()
                .field("time", currentTime)
                .field("content", line)
              .endObject()
            ).execute().actionGet();
  }

  public String Run() throws IOException, InterruptedException {
    status = "running";
    String[] splits = cmd.split(" ");
    ProcessBuilder processBuilder = new ProcessBuilder(splits);

    process = processBuilder.start();
    InputStream stream = process.getInputStream();
    process.waitFor();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    
    String line, output = "";
    while ((line = reader.readLine()) != null) {
      WriteRunningStatus(line);
      output += line + "\n";
    }
    
    return output;
  }

  public Task(Map<String, String> taskMap, Client client, String indexName) {
    this.id = taskMap.get("id");
    this.cmd = taskMap.get("cmd");
    this.priority = taskMap.get("priority");
    this.status = taskMap.get("status");
    this.client = client;
    this.indexName = indexName;
  }
}
