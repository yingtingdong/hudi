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

package org.apache.hudi.config.metrics;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.config.metrics.HoodieMetricsConfig.METRIC_PREFIX;

@ConfigClassProperty(name = "Metrics Configurations for Zhiyan",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics using Zhiyan. "
      + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsZhiyanConfig extends HoodieConfig {

  public static final String ZHIYAN_PREFIX = METRIC_PREFIX + ".zhiyan";

  public static final ConfigProperty<Integer> API_TIMEOUT_IN_SECONDS = ConfigProperty
      .key(ZHIYAN_PREFIX + ".api.timeout.seconds")
      .defaultValue(10)
      .sinceVersion("0.10.0")
      .withDocumentation("Zhiyan API timeout in seconds. Default to 10.");

  public static final ConfigProperty<Integer> REPORT_PERIOD_SECONDS = ConfigProperty
      .key(ZHIYAN_PREFIX + ".report.period.seconds")
      .defaultValue(10)
      .sinceVersion("0.10.0")
      .withDocumentation("Zhiyan Report period seconds. Default to 10.");

  public static final ConfigProperty<String> REPORT_SERVICE_URL = ConfigProperty
      .key(ZHIYAN_PREFIX + ".report.service.url")
      .defaultValue("http://zhiyan.monitor.access.inner.woa.com:8080")
      .withDocumentation("Zhiyan Report service url.");

  public static final ConfigProperty<String> REPORT_SERVICE_PATH = ConfigProperty
      .key(ZHIYAN_PREFIX + ".report.service.path")
      .defaultValue("/access_v1.http_service/HttpCurveReportRpc")
      .withDocumentation("Zhiyan Report service path.");

  public static final ConfigProperty<String> ZHIYAN_JOB_NAME = ConfigProperty
      .key(ZHIYAN_PREFIX + ".job.name")
      .defaultValue("")
      .sinceVersion("0.10.0")
      .withDocumentation("Name of Job using zhiyan metrics reporter.");

  public static final ConfigProperty<Boolean> ZHIYAN_RANDOM_JOBNAME_SUFFIX = ConfigProperty
      .key(ZHIYAN_PREFIX + ".random.job.name.suffix")
      .defaultValue(true)
      .sinceVersion("0.10.0")
      .withDocumentation("Whether the Zhiyan job name need a random suffix , default true.");

  public static final ConfigProperty<String> ZHIYAN_METRICS_HOODIE_APPMASK = ConfigProperty
      .key(ZHIYAN_PREFIX + ".hoodie.appmask")
      .defaultValue("1701_36311_HUDI")
      .sinceVersion("0.10.0")
      .withDocumentation("Zhiyan appmask for hudi.");

  public static final ConfigProperty<String> ZHIYAN_METRICS_HOODIE_SECLVLENNAME = ConfigProperty
      .key(ZHIYAN_PREFIX + ".hoodie.seclvlenname")
      .defaultValue("hudi_metrics")
      .sinceVersion("0.10.0")
      .withDocumentation("Zhiyan seclvlenvname for hudi, default hudi_metrics");

  public static Builder newBuilder() {
    return new HoodieMetricsZhiyanConfig.Builder();
  }

  public static class Builder {

    private final HoodieMetricsZhiyanConfig hoodieMetricsZhiyanConfig = new HoodieMetricsZhiyanConfig();

    public HoodieMetricsZhiyanConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieMetricsZhiyanConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieMetricsZhiyanConfig.Builder fromProperties(Properties props) {
      this.hoodieMetricsZhiyanConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsZhiyanConfig.Builder withAppMask(String appMask) {
      hoodieMetricsZhiyanConfig.setValue(ZHIYAN_METRICS_HOODIE_APPMASK, appMask);
      return this;
    }

    public HoodieMetricsZhiyanConfig.Builder withSeclvlEnvName(String seclvlEnvName) {
      hoodieMetricsZhiyanConfig.setValue(ZHIYAN_METRICS_HOODIE_SECLVLENNAME, seclvlEnvName);
      return this;
    }

    public HoodieMetricsZhiyanConfig.Builder withReportServiceUrl(String url) {
      hoodieMetricsZhiyanConfig.setValue(REPORT_SERVICE_URL, url);
      return this;
    }

    public HoodieMetricsZhiyanConfig.Builder withApiTimeout(int apiTimeout) {
      hoodieMetricsZhiyanConfig.setValue(API_TIMEOUT_IN_SECONDS, String.valueOf(apiTimeout));
      return this;
    }

    public HoodieMetricsZhiyanConfig.Builder withJobName(String jobName) {
      hoodieMetricsZhiyanConfig.setValue(ZHIYAN_JOB_NAME, jobName);
      return this;
    }

    public HoodieMetricsZhiyanConfig.Builder withReportPeriodSeconds(int seconds) {
      hoodieMetricsZhiyanConfig.setValue(REPORT_PERIOD_SECONDS, String.valueOf(seconds));
      return this;
    }

    public HoodieMetricsZhiyanConfig build() {
      hoodieMetricsZhiyanConfig.setDefaults(HoodieMetricsZhiyanConfig.class.getName());
      return hoodieMetricsZhiyanConfig;
    }
  }

}
