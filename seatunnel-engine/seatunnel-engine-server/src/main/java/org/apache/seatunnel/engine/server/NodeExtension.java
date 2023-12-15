/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.log.Log4j2HttpPostCommandProcessor;
import org.apache.seatunnel.engine.server.rest.RestHttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.rest.RestHttpPostCommandProcessor;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;
import lombok.NonNull;

import java.util.Map;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_GET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_POST;

public class NodeExtension extends DefaultNodeExtension {
    private final NodeExtensionCommon extCommon;

    /**
     * NodeExtension 核心依次调用其
     * 1 printNodeInfo()
     * 2 beforeStart()
     * 3 createTextCommandService()
     * 4 afterStart()
     */
    public NodeExtension(@NonNull Node node, @NonNull SeaTunnelConfig seaTunnelConfig) {
        // 调用父类
        super(node);
        // 创建 NodeExtensionCommon, SeaTunnelServer
        extCommon = new NodeExtensionCommon(node, new SeaTunnelServer(seaTunnelConfig));
    }

    @Override
    public void beforeStart() {
        // TODO Get Config from Node here
        super.beforeStart();
    }

    @Override
    public void afterStart() {
        super.afterStart();
        extCommon.afterStart();
    }

    @Override
    public void beforeClusterStateChange(
            ClusterState currState, ClusterState requestedState, boolean isTransient) {
        super.beforeClusterStateChange(currState, requestedState, isTransient);
        extCommon.beforeClusterStateChange(requestedState);
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        super.onClusterStateChange(newState, isTransient);
        extCommon.onClusterStateChange(newState);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        // 往 Hazelcast 的 ServiceManager 注册 SeaTunnelServer
        return extCommon.createExtensionServices();
    }

    @Override
    public TextCommandService createTextCommandService() {
        // 创建 TextCommandService
        return new TextCommandServiceImpl(node) {
            {
                register(HTTP_GET, new Log4j2HttpGetCommandProcessor(this));
                register(HTTP_POST, new Log4j2HttpPostCommandProcessor(this));
                register(HTTP_GET, new RestHttpGetCommandProcessor(this));
                register(HTTP_POST, new RestHttpPostCommandProcessor(this));
            }
        };
    }

    @Override
    public void printNodeInfo() {
        extCommon.printNodeInfo(systemLogger);
    }
}
