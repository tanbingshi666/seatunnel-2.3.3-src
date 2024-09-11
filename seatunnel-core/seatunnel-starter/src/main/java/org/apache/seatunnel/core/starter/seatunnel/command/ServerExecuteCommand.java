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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.instance.impl.HazelcastInstanceFactory;

/**
 * This command is used to execute the SeaTunnel engine job by SeaTunnel API.
 */
public class ServerExecuteCommand implements Command<ServerCommandArgs> {

    private final ServerCommandArgs serverCommandArgs;

    public ServerExecuteCommand(ServerCommandArgs serverCommandArgs) {
        this.serverCommandArgs = serverCommandArgs;
    }

    @Override
    public void execute() {
        // 1 加载 seatunnel.yaml 和 hazelcast.yaml 配置文件 统一封装为 SeaTunnelConfig 对象
        // seatunnel.yaml 负责 seatunnel-engine 配置 统一封装为 EngineConfig 对象
        // hazelcast.yaml 负责 hazelcast-imdg 配置 统一封装为 Config
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();

        // 2 是否在命令参数执行 --cluster xxx 参数
        if (StringUtils.isNotEmpty(serverCommandArgs.getClusterName())) {
            seaTunnelConfig.getHazelcastConfig().setClusterName(serverCommandArgs.getClusterName());
        }

        // 3 启动 seatunnel-engine 服务
        HazelcastInstanceFactory.newHazelcastInstance(
                // hazelcast 配置
                seaTunnelConfig.getHazelcastConfig(),
                Thread.currentThread().getName(),
                // 创建 SeaTunnel 节点上下文
                new SeaTunnelNodeContext(seaTunnelConfig));
    }
}
