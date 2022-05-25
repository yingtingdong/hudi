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

package org.apache.hudi.metrics.zhiyan;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.MetricsReporter;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class ZhiyanMetricsReporter extends MetricsReporter {

  private final ZhiyanReporter reporter;
  private final int reportPeriodSeconds;

  public ZhiyanMetricsReporter(HoodieWriteConfig config, MetricRegistry registry) {
    this.reportPeriodSeconds = config.getZhiyanReportPeriodSeconds();
    ZhiyanHttpClient client = new ZhiyanHttpClient(
        config.getZhiyanReportServiceURL(),
        config.getZhiyanReportServicePath(),
        config.getZhiyanApiTimeoutSeconds());
    this.reporter = new ZhiyanReporter(registry, MetricFilter.ALL, client,
        config.getZhiyanHoodieJobName(),
        config.getTableName(),
        config.getZhiyanAppMask(),
        config.getZhiyanSeclvlEnvName());
  }

  @Override
  public void start() {
    reporter.start(reportPeriodSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void report() {
    reporter.report();
  }

  @Override
  public Closeable getReporter() {
    return reporter;
  }

  @Override
  public void stop() {
    reporter.stop();
  }
}
