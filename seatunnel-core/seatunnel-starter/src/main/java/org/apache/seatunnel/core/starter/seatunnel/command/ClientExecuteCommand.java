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

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import org.apache.commons.lang3.StringUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

/**
 * This command is used to execute the SeaTunnel engine job by SeaTunnel API.
 */
@Slf4j
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    private JobStatus jobStatus;
    private SeaTunnelClient engineClient;
    private HazelcastInstance instance;
    private ScheduledExecutorService executorService;

    public ClientExecuteCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:MagicNumber"})
    @Override
    public void execute() throws CommandExecuteException {
        JobMetricsRunner.JobMetricsSummary jobMetricsSummary = null;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();

        // 1 加载 seatunnel.yaml 和 hazelcast.yaml 文件 (文件在 seatunnel-engine-common 模块)
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        try {

            // 2 从客户端参数中获取集群名称 比如命令参数设置 --cluster xxx
            String clusterName = clientCommandArgs.getClusterName();

            // 3 如果客户端提交 Job 是 Local 模式情况下 也即命令参数设置 -e local
            if (clientCommandArgs.getMasterType().equals(MasterType.LOCAL)) {
                // 随机生成一个集群名称
                clusterName =
                        creatRandomClusterName(
                                StringUtils.isNotEmpty(clusterName)
                                        ? clusterName
                                        : Constant.DEFAULT_SEATUNNEL_CLUSTER_NAME);
                // 基于 NIO 启动一个 ServerSocketChannel 服务
                /**
                 * 1 seatunnel 网络 RPC 基于 Hazelcast 框架封装 NIO ServerSocketChannel
                 * 2 seatunnel 的 SeaTunnelNodeContext 继承 Hazelcast 框架的 DefaultNodeContext
                 *   SeaTunnelNodeContext 将是 seatunnel RPC 服务的上下文 也即封装了 SeatunnelServer
                 * 3 SeaTunnelNodeContext 创建 NodeExtension, NodeExtension 创建 NodeExtensionCommon
                 *   NodeExtensionCommon 创建 SeaTunnelServer
                 * 4 SeaTunnelServer 是 seatunnel-engine 服务, 同时也是 Hazelcast 的服务 故会执行 init()
                 */
                instance = createServerInLocal(clusterName, seaTunnelConfig);
            }

            if (StringUtils.isNotEmpty(clusterName)) {
                seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            }
            // 用户客户端加载 hazelcast-client.yaml 文件
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            if (StringUtils.isNotEmpty(clusterName)) {
                clientConfig.setClusterName(clusterName);
            }
            // 5 创建客户端与服务端的 ServerSocketChannel 交互 也即连接
            engineClient = new SeaTunnelClient(clientConfig);
            // 用户参数设置存在 --list 情况下
            if (clientCommandArgs.isListJob()) {
                String jobStatus = engineClient.getJobClient().listJobStatus(true);
                System.out.println(jobStatus);

                // 用户参数设置存在 --get_running_job_metrics 情况下
            } else if (clientCommandArgs.isGetRunningJobMetrics()) {
                String runningJobMetrics = engineClient.getJobClient().getRunningJobMetrics();
                System.out.println(runningJobMetrics);

                // 用户参数设置存在 --job-id 情况下
            } else if (null != clientCommandArgs.getJobId()) {
                String jobState =
                        engineClient
                                .getJobClient()
                                .getJobDetailStatus(Long.parseLong(clientCommandArgs.getJobId()));
                System.out.println(jobState);

                // 用户参数设置存在 --cancel-job 情况下
            } else if (null != clientCommandArgs.getCancelJobId()) {
                engineClient
                        .getJobClient()
                        .cancelJob(Long.parseLong(clientCommandArgs.getCancelJobId()));

                // 用户参数设置存在 --metrics 情况下
            } else if (null != clientCommandArgs.getMetricsJobId()) {
                String jobMetrics =
                        engineClient
                                .getJobClient()
                                .getJobMetrics(Long.parseLong(clientCommandArgs.getMetricsJobId()));
                System.out.println(jobMetrics);

                // 用户参数设置存在 --savepoint 情况下
            } else if (null != clientCommandArgs.getSavePointJobId()) {
                engineClient
                        .getJobClient()
                        .savePointJob(Long.parseLong(clientCommandArgs.getSavePointJobId()));
            } else {
                // 6 客户端向服务端提交 Job

                // 6.1 获取用户执行 Job 配置文件路径
                Path configFile = FileUtils.getConfigPath(clientCommandArgs);
                checkConfigExist(configFile);
                JobConfig jobConfig = new JobConfig();
                JobExecutionEnvironment jobExecutionEnv;
                // 6.2 设置用户执行 Job 名称
                jobConfig.setName(clientCommandArgs.getJobName());
                // 6.3 用户执行 Job 是否为 restore
                if (null != clientCommandArgs.getRestoreJobId()) {
                    // restore Job 情况下
                    jobExecutionEnv =
                            engineClient.restoreExecutionContext(
                                    configFile.toString(),
                                    jobConfig,
                                    Long.parseLong(clientCommandArgs.getRestoreJobId()));
                } else {
                    // 6.4 new Job 情况下 创建 Job 执行环境 JobExecutionEnvironment
                    jobExecutionEnv = engineClient.createExecutionContext(configFile.toString(), jobConfig);
                }

                // get job start time
                startTime = LocalDateTime.now();
                // create job proxy
                // 6.5 创建 Job 代理 ClientJobProxy 提交 Job
                // 服务端调用 CoordinatorService.submitJob() 执行
                ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
                if (clientCommandArgs.isAsync()) {
                    if (clientCommandArgs.getMasterType().equals(MasterType.LOCAL)) {
                        log.warn("The job is running in local mode, can not use async mode.");
                    } else {
                        return;
                    }
                }
                // register cancelJob hook
                Runtime.getRuntime()
                        .addShutdownHook(
                                new Thread(
                                        () -> {
                                            CompletableFuture<Void> future =
                                                    CompletableFuture.runAsync(
                                                            () -> {
                                                                log.info(
                                                                        "run shutdown hook because get close signal");
                                                                shutdownHook(clientJobProxy);
                                                            });
                                            try {
                                                future.get(15, TimeUnit.SECONDS);
                                            } catch (Exception e) {
                                                log.error("Cancel job failed.", e);
                                            }
                                        }));
                // get job id
                long jobId = clientJobProxy.getJobId();
                JobMetricsRunner jobMetricsRunner = new JobMetricsRunner(engineClient, jobId);
                executorService =
                        Executors.newSingleThreadScheduledExecutor(
                                new ThreadFactoryBuilder()
                                        .setNameFormat("job-metrics-runner-%d")
                                        .setDaemon(true)
                                        .build());
                executorService.scheduleAtFixedRate(
                        jobMetricsRunner,
                        0,
                        seaTunnelConfig.getEngineConfig().getPrintJobMetricsInfoInterval(),
                        TimeUnit.SECONDS);
                // wait for job complete
                jobStatus = clientJobProxy.waitForJobComplete();
                // get job end time
                endTime = LocalDateTime.now();
                // get job statistic information when job finished
                jobMetricsSummary = engineClient.getJobMetricsSummary(jobId);
            }
        } catch (Exception e) {
            throw new CommandExecuteException("SeaTunnel job executed failed", e);
        } finally {
            if (jobMetricsSummary != null) {
                // print job statistics information when job finished
                log.info(
                        StringFormatUtils.formatTable(
                                "Job Statistic Information",
                                "Start Time",
                                DateTimeUtils.toString(
                                        startTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                                "End Time",
                                DateTimeUtils.toString(
                                        endTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                                "Total Time(s)",
                                Duration.between(startTime, endTime).getSeconds(),
                                "Total Read Count",
                                jobMetricsSummary.getSourceReadCount(),
                                "Total Write Count",
                                jobMetricsSummary.getSinkWriteCount(),
                                "Total Failed Count",
                                jobMetricsSummary.getSourceReadCount()
                                        - jobMetricsSummary.getSinkWriteCount()));
            }
            closeClient();
        }
    }

    private void closeClient() {
        if (engineClient != null) {
            engineClient.close();
            log.info("Closed SeaTunnel client......");
        }
        if (instance != null) {
            instance.shutdown();
            log.info("Closed HazelcastInstance ......");
        }
        if (executorService != null) {
            executorService.shutdownNow();
            log.info("Closed metrics executor service ......");
        }
    }

    private HazelcastInstance createServerInLocal(
            String clusterName, SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        return HazelcastInstanceFactory.newHazelcastInstance(
                seaTunnelConfig.getHazelcastConfig(),
                Thread.currentThread().getName(),
                new SeaTunnelNodeContext(seaTunnelConfig));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private String creatRandomClusterName(String namePrefix) {
        Random random = new Random();
        return namePrefix + "-" + random.nextInt(1000000);
    }

    private void shutdownHook(ClientJobProxy clientJobProxy) {
        if (clientCommandArgs.isCloseJob()) {
            if (clientJobProxy.getJobResultCache() == null
                    && (jobStatus == null || !jobStatus.isEndState())) {
                log.warn("Task will be closed due to client shutdown.");
                clientJobProxy.cancelJob();
            }
        }
    }
}
