package com.clearedgeit.accumulo.flume;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.json.*;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.io.Text;

/**
 * BinaryEdge's implemention of the AccumuloEventSerializer interface.
 * 
 * rowId, columnFamily, columnVisibility and columnQualifier are set according to the
 * content of the event body which we assume to be a valid json string with predefined
 * fields.
 */

public class BinaryEdgeAccumuloEventSerializer implements AccumuloEventSerializer {
  
  private Event currentEvent;
  
  @Override
  public void configure(Context arg0) {

  }
  
  @Override
  public void configure(ComponentConfiguration arg0) {

  }
  
  @Override
  public void set(Event event) {
    this.currentEvent = event;
  }
  
  @Override
  public List<Mutation> getMutations() {
    
    List<Mutation> mutationList = new LinkedList<Mutation>();
    
    Map<String,String> headers = this.currentEvent.getHeaders();

    // If rowID, columnVisibility, columnFamily or columnQualifier 
    // are set in the headers of the event, those values will be used 
    // (and removed from the headers map so they don't get written to 
    // accumulo as actual values)
    String rowID_header = null;
    String columnVisibility_header = null;
    String columnFamily_header = null;
    String columnQualifier_header = null;
    if (headers != null) {
      if (headers.containsKey("rowID")) {
        rowID_header = headers.remove("rowID");
      }
      if (headers.containsKey("columnFamily")) {
        columnFamily_header = headers.remove("columnFamily");
      }
      if (headers.containsKey("columnQualifier")) {
        columnQualifier_header = headers.remove("columnQualifier");
      }
      if (headers.containsKey("columnVisibility")) {
        columnVisibility_header = headers.remove("columnVisibility");
      }
    }

    byte[] raw_body = this.currentEvent.getBody();
    JSONObject parsed_body = new JSONObject(new String(raw_body));
    
    Text rowID = new Text();
    if (rowID_header != null && rowID_header.length() > 0) {
      rowID.set(rowID_header);
    } else {
      JSONObject src = parsed_body.getJSONObject("src");
      rowID.set("ip_" + src.getString("ip") + "_" + src.getString("port") + "_" + src.getString("protocol"));
    }

    Text columnFamily = new Text();
    if (columnFamily_header != null && columnFamily_header.length() > 0) {
      columnFamily.set(columnFamily_header);
    } else {
      columnFamily.set(parsed_body.getString("result_type"));
    }
    
    Text columnQualifier = new Text();
    if (columnQualifier_header != null && columnQualifier_header.length() > 0) {
      columnQualifier.set(columnQualifier_header);
    } else {
      columnQualifier.set("raw");
    }

    ColumnVisibility columnVisibility = null;
    if (columnVisibility_header != null && columnVisibility_header.length() > 0) {
      columnVisibility = new ColumnVisibility(columnVisibility_header.getBytes());
    } else {
      columnVisibility = new ColumnVisibility();
    }
    
    Value value = new Value(parsed_body.getJSONObject("result").toString().getBytes());
    Mutation mutation = new Mutation(rowID);
    mutation.put(columnFamily, columnQualifier, columnVisibility, value);
    mutationList.add(mutation);
    
    return mutationList;
  }
  
  @Override
  public void close() {
    this.currentEvent = null;
  }
  
}
