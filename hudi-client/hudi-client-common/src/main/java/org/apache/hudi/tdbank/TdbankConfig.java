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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;
import java.util.Properties;

@Immutable
@ConfigClassProperty(name = "Tdbank Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Tdbank configs")
public class TdbankConfig extends HoodieConfig {
  public static final ConfigProperty<String> TDBANK_TDM_ADDR = ConfigProperty
      .key("hoodie.tdbank.tdm.addr")
      .defaultValue("tl-tdbank-tdmanager.tencent-distribute.com")
      .withDocumentation("tdbank manager address.");

  public static final ConfigProperty<Integer> TDBANK_TDM_PORT = ConfigProperty
      .key("hoodie.tdbank.tdm.port")
      .defaultValue(8099)
      .withDocumentation("tdbank manager port.");

  public static final ConfigProperty<String> TDBANK_BID = ConfigProperty
      .key("hoodie.tdbank.tdbank.bid")
      .defaultValue("b_teg_iceberg_event_tdbank_mq")
      .withDocumentation("tdbank bid, use iceberg's bid temporarily.");

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final TdbankConfig hoodieTdbankConfig = new TdbankConfig();

    public Builder withTDMAddr(String tdmAddr) {
      hoodieTdbankConfig.setValue(TDBANK_TDM_ADDR, tdmAddr);
      return this;
    }

    public Builder fromProperties(Properties props) {
      hoodieTdbankConfig.setAll(props);
      return this;
    }

    public Builder withTDMPort(int tdmPort) {
      hoodieTdbankConfig.setValue(TDBANK_TDM_PORT, String.valueOf(tdmPort));
      return this;
    }

    public Builder withBID(String bid) {
      hoodieTdbankConfig.setValue(TDBANK_BID, bid);
      return this;
    }

    public TdbankConfig build() {
      hoodieTdbankConfig.setDefaults(TdbankConfig.class.getName());
      return hoodieTdbankConfig;
    }
  }

}
