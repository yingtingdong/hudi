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

package org.apache.hudi.async;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.tdbank.TDBankClient;
import org.apache.hudi.tdbank.TdbankHoodieMetricsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Async service to post event to remote service..
 */
public class AsyncPostEventService extends HoodieAsyncService {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncPostEventService.class);

  private final transient ExecutorService executor = Executors.newSingleThreadExecutor();
  private final LinkedBlockingQueue<TdbankHoodieMetricsEvent> queue;
  private final TDBankClient client;

  public AsyncPostEventService(HoodieWriteConfig config, LinkedBlockingQueue<TdbankHoodieMetricsEvent> queue) {
    this.client = new TDBankClient(config.getTdbankTdmAddr(),
      config.getTdbankTdmPort(), config.getTdbankBid());
    this.queue = queue;
  }

  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    LOG.info("Start async post event service...");
    return Pair.of(CompletableFuture.supplyAsync(() -> {
      sendEvent();
      return true;
    }, executor), executor);
  }

  private void sendEvent() {
    try {
      while (!isShutdownRequested()) {
        TdbankHoodieMetricsEvent event = queue.poll(10, TimeUnit.SECONDS);
        if (event != null) {
          client.sendMessage(event);
        }
      }
      LOG.info("Post event service shutdown properly.");
    } catch (Exception e) {
      LOG.error("Error when send event to tdbank", e);
    }
  }

  // TODO simplfy codes here among async package.
  public static void waitForCompletion(AsyncArchiveService asyncArchiveService) {
    if (asyncArchiveService != null) {
      LOG.info("Waiting for async archive service to finish");
      try {
        asyncArchiveService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException("Error waiting for async archive service to finish", e);
      }
    }
  }

  public static void forceShutdown(AsyncArchiveService asyncArchiveService) {
    if (asyncArchiveService != null) {
      LOG.info("Shutting down async archive service...");
      asyncArchiveService.shutdown(true);
    }
  }
}
