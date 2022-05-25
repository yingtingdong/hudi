/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.tdbank;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class TdbankHoodieMetricsEvent implements Serializable {
  private String dbName;
  private String tableName;
  private EventType type;
  private Map<String, Object> metrics;

  private TdbankHoodieMetricsEvent() {
    this.metrics = new TreeMap<>();
  }

  public enum EventType {
    INDEX, CLEAN, FINALIZE, ROLLBACK, COMPACTION, COMMIT, DELTACOMMIT, REPLACECOMMIT
  }

  public static TdbankHoodieMetricsEvent.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final TdbankHoodieMetricsEvent hoodieMetricsEvent = new TdbankHoodieMetricsEvent();

    public Builder() {
    }

    public Builder withDBName(String dbName) {
      hoodieMetricsEvent.setDbName(dbName);
      return this;
    }

    public Builder withTableName(String tableName) {
      hoodieMetricsEvent.setTableName(tableName);
      return this;
    }

    public Builder withTableType(EventType type) {
      hoodieMetricsEvent.setType(type);
      return this;
    }

    public Builder addMetrics(String key, Object value) {
      hoodieMetricsEvent.addMetrics(key, value);
      return this;
    }

    public TdbankHoodieMetricsEvent build() {
      return hoodieMetricsEvent;
    }
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setType(EventType type) {
    this.type = type;
  }

  public void addMetrics(String key, Object value) {
    this.metrics.put(key, value);
  }

  public String getTableName() {
    return tableName;
  }

  public EventType getType() {
    return type;
  }

  public Map<String, Object> getMetrics() {
    return metrics;
  }

  public Object getMetric(String key) {
    return metrics.get(key);
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }
}
