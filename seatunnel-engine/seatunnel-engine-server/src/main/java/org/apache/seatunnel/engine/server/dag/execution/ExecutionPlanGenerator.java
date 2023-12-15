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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleConfig;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleMultipleRowStrategy;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleStrategy;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ExecutionPlanGenerator {
    private final LogicalDag logicalPlan;
    private final JobImmutableInformation jobImmutableInformation;
    private final CheckpointConfig checkpointConfig;
    private final IdGenerator idGenerator = new IdGenerator();

    public ExecutionPlanGenerator(
            @NonNull LogicalDag logicalPlan,
            @NonNull JobImmutableInformation jobImmutableInformation,
            @NonNull CheckpointConfig checkpointConfig) {
        checkArgument(
                logicalPlan.getEdges().size() > 0, "ExecutionPlan Builder must have LogicalPlan.");
        this.logicalPlan = logicalPlan;
        this.jobImmutableInformation = jobImmutableInformation;
        this.checkpointConfig = checkpointConfig;
    }

    public ExecutionPlan generate() {
        log.debug("Generate execution plan using logical plan:");

        // 1 将 LogicalEdge 的上下游的 LogicalVertex 转换为 ExecutionVertex
        // 将 LogicalEdge 转换为 ExecutionEdge 连接上下游 ExecutionVertex
        // Phase 1: generate execution edge list
        // [ExecutionEdge(
        // leftVertex=ExecutionVertex(vertexId=1, action=org.apache.seatunnel.engine.core.dag.actions.SourceAction@39e36b11, parallelism=1),
        // rightVertex=ExecutionVertex(vertexId=2, action=org.apache.seatunnel.engine.core.dag.actions.SinkAction@fd159e6, parallelism=1),
        // leftVertexId=1, rightVertexId=2)]
        Set<ExecutionEdge> executionEdges = generateExecutionEdges(logicalPlan.getEdges());
        log.debug("Phase 1: generate execution edge list {}", executionEdges);

        // Phase 2: generate shuffle edge list
        // [ExecutionEdge(leftVertex=ExecutionVertex(vertexId=1, action=org.apache.seatunnel.engine.core.dag.actions.SourceAction@39e36b11, parallelism=1),
        // rightVertex=ExecutionVertex(vertexId=2, action=org.apache.seatunnel.engine.core.dag.actions.SinkAction@fd159e6, parallelism=1),
        // leftVertexId=1, rightVertexId=2)]
        executionEdges = generateShuffleEdges(executionEdges);
        log.debug("Phase 2: generate shuffle edge list {}", executionEdges);

        // Phase 3: generate transform chain edge list
        // [ExecutionEdge(leftVertex=ExecutionVertex(vertexId=1, action=org.apache.seatunnel.engine.core.dag.actions.SourceAction@39e36b11, parallelism=1),
        // rightVertex=ExecutionVertex(vertexId=2, action=org.apache.seatunnel.engine.core.dag.actions.SinkAction@fd159e6, parallelism=1),
        // leftVertexId=1, rightVertexId=2)]
        executionEdges = generateTransformChainEdges(executionEdges);
        log.debug("Phase 3: generate transform chain edge list {}", executionEdges);

        // 将不关联的 ExecutionEdge 切分为不同的 Pipeline
        List<Pipeline> pipelines = generatePipelines(executionEdges);
        log.debug("Phase 4: generate pipeline list {}", pipelines);

        // 创建 ExecutionPlan
        ExecutionPlan executionPlan = new ExecutionPlan(pipelines, jobImmutableInformation);
        log.debug("Phase 5: generate execution plan: {}", executionPlan);

        return executionPlan;
    }

    public static Action recreateAction(Action action, Long id, int parallelism) {
        Action newAction;
        if (action instanceof ShuffleAction) {
            newAction =
                    new ShuffleAction(id, action.getName(), ((ShuffleAction) action).getConfig());
        } else if (action instanceof SinkAction) {
            newAction =
                    new SinkAction<>(
                            id,
                            action.getName(),
                            new ArrayList<>(),
                            ((SinkAction<?, ?, ?, ?>) action).getSink(),
                            action.getJarUrls(),
                            (SinkConfig) action.getConfig());
        } else if (action instanceof SourceAction) {
            newAction =
                    new SourceAction<>(
                            id,
                            action.getName(),
                            ((SourceAction<?, ?, ?>) action).getSource(),
                            action.getJarUrls());
        } else if (action instanceof TransformAction) {
            newAction =
                    new TransformAction(
                            id,
                            action.getName(),
                            ((TransformAction) action).getTransform(),
                            action.getJarUrls());
        } else if (action instanceof TransformChainAction) {
            newAction =
                    new TransformChainAction(
                            id,
                            action.getName(),
                            action.getJarUrls(),
                            ((TransformChainAction<?>) action).getTransforms());
        } else {
            throw new UnknownActionException(action);
        }
        newAction.setParallelism(parallelism);
        return newAction;
    }

    private Set<ExecutionEdge> generateExecutionEdges(Set<LogicalEdge> logicalEdges) {
        Set<ExecutionEdge> executionEdges = new LinkedHashSet<>();

        Map<Long, ExecutionVertex> logicalVertexIdToExecutionVertexMap = new HashMap();

        // 1 LogicalDag 的 LogicalEdges 根据 inputVertexId 和 outputVertexId 依次降序排序
        List<LogicalEdge> sortedLogicalEdges = new ArrayList<>(logicalEdges);
        Collections.sort(
                sortedLogicalEdges,
                (o1, o2) -> {
                    if (o1.getInputVertexId() != o2.getInputVertexId()) {
                        return o1.getInputVertexId() > o2.getInputVertexId() ? 1 : -1;
                    }
                    if (o1.getTargetVertexId() != o2.getTargetVertexId()) {
                        return o1.getTargetVertexId() > o2.getTargetVertexId() ? 1 : -1;
                    }
                    return 0;
                });

        // 2 遍历已经排序好的 LogicalEdge
        for (LogicalEdge logicalEdge : sortedLogicalEdges) {
            // 2.1 获取 LogicalEdge 的上游 LogicalVertex 转换为 ExecutionVertex
            LogicalVertex logicalInputVertex = logicalEdge.getInputVertex();
            ExecutionVertex executionInputVertex =
                    logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                            logicalInputVertex.getVertexId(),
                            vertexId -> {
                                long newId = idGenerator.getNextId();
                                Action newLogicalInputAction =
                                        recreateAction(
                                                logicalInputVertex.getAction(),
                                                newId,
                                                logicalInputVertex.getParallelism());
                                return new ExecutionVertex(
                                        newId,
                                        newLogicalInputAction,
                                        logicalInputVertex.getParallelism());
                            });

            // 2.2 获取 LogicalEdge 的下游 LogicalVertex 转换为 ExecutionVertex
            LogicalVertex logicalTargetVertex = logicalEdge.getTargetVertex();
            ExecutionVertex executionTargetVertex =
                    logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                            logicalTargetVertex.getVertexId(),
                            vertexId -> {
                                long newId = idGenerator.getNextId();
                                Action newLogicalTargetAction =
                                        recreateAction(
                                                logicalTargetVertex.getAction(),
                                                newId,
                                                logicalTargetVertex.getParallelism());
                                return new ExecutionVertex(
                                        newId,
                                        newLogicalTargetAction,
                                        logicalTargetVertex.getParallelism());
                            });

            // 3 创建 ExecutionEdge 连接上下游 ExecutionVertex
            ExecutionEdge executionEdge =
                    new ExecutionEdge(executionInputVertex, executionTargetVertex);
            executionEdges.add(executionEdge);
        }
        return executionEdges;
    }

    @SuppressWarnings("MagicNumber")
    private Set<ExecutionEdge> generateShuffleEdges(Set<ExecutionEdge> executionEdges) {
        // 1 遍历每个 ExecutionEdge 即其上游 ExecutionVertex 缓存在 sourceExecutionVertices 集合
        // 同时将上游的 ExecutionVertex 的 ID 映射下游的 ExecutionVertex 缓存在 targetVerticesMap
        Map<Long, List<ExecutionVertex>> targetVerticesMap = new LinkedHashMap<>();
        Set<ExecutionVertex> sourceExecutionVertices = new HashSet<>();
        executionEdges.forEach(
                edge -> {
                    ExecutionVertex leftVertex = edge.getLeftVertex();
                    ExecutionVertex rightVertex = edge.getRightVertex();
                    if (leftVertex.getAction() instanceof SourceAction) {
                        sourceExecutionVertices.add(leftVertex);
                    }
                    targetVerticesMap
                            .computeIfAbsent(leftVertex.getVertexId(), id -> new ArrayList<>())
                            .add(rightVertex);
                });
        if (sourceExecutionVertices.size() != 1) {
            return executionEdges;
        }

        // 2 获取第一个 source 的 ExecutionVertex
        // todo ExecutionVertex 没有排序是否返回随机的 ExecutionVertex
        ExecutionVertex sourceExecutionVertex = sourceExecutionVertices.stream().findFirst().get();
        SourceAction sourceAction = (SourceAction) sourceExecutionVertex.getAction();
        SeaTunnelDataType sourceProducedType = sourceAction.getSource().getProducedType();
        if (!SqlType.MULTIPLE_ROW.equals(sourceProducedType.getSqlType())) {
            return executionEdges;
        }

        // 3 获取下游 ExecutionVertex
        List<ExecutionVertex> sinkVertices =
                targetVerticesMap.get(sourceExecutionVertex.getVertexId());
        Optional<ExecutionVertex> hasOtherAction =
                sinkVertices.stream()
                        .filter(vertex -> !(vertex.getAction() instanceof SinkAction))
                        .findFirst();
        checkArgument(!hasOtherAction.isPresent());

        // 4 构建 ShuffleStrategy 和 ShuffleConfig
        Set<ExecutionEdge> newExecutionEdges = new LinkedHashSet<>();
        ShuffleStrategy shuffleStrategy =
                ShuffleMultipleRowStrategy.builder()
                        .jobId(jobImmutableInformation.getJobId())
                        .inputPartitions(sourceAction.getParallelism())
                        .inputRowType(MultipleRowType.class.cast(sourceProducedType))
                        .queueEmptyQueueTtl((int) (checkpointConfig.getCheckpointInterval() * 3))
                        .build();
        ShuffleConfig shuffleConfig = ShuffleConfig.builder().shuffleStrategy(shuffleStrategy).build();

        long shuffleVertexId = idGenerator.getNextId();
        String shuffleActionName = String.format("Shuffle [%s]", sourceAction.getName());

        // 5 创建 ShuffleAction
        ShuffleAction shuffleAction = new ShuffleAction(shuffleVertexId, shuffleActionName, shuffleConfig);
        shuffleAction.setParallelism(sourceAction.getParallelism());

        // 6 创建 ExecutionVertex 和 ExecutionEdge
        ExecutionVertex shuffleVertex = new ExecutionVertex(shuffleVertexId, shuffleAction, shuffleAction.getParallelism());
        ExecutionEdge sourceToShuffleEdge = new ExecutionEdge(sourceExecutionVertex, shuffleVertex);
        newExecutionEdges.add(sourceToShuffleEdge);

        for (ExecutionVertex sinkVertex : sinkVertices) {
            sinkVertex.setParallelism(1);
            sinkVertex.getAction().setParallelism(1);
            ExecutionEdge shuffleToSinkEdge = new ExecutionEdge(shuffleVertex, sinkVertex);
            newExecutionEdges.add(shuffleToSinkEdge);
        }

        return newExecutionEdges;
    }

    private Set<ExecutionEdge> generateTransformChainEdges(Set<ExecutionEdge> executionEdges) {
        Map<Long, List<ExecutionVertex>> inputVerticesMap = new HashMap<>();
        Map<Long, List<ExecutionVertex>> targetVerticesMap = new HashMap<>();
        Set<ExecutionVertex> sourceExecutionVertices = new HashSet<>();
        executionEdges.forEach(
                edge -> {
                    ExecutionVertex leftVertex = edge.getLeftVertex();
                    ExecutionVertex rightVertex = edge.getRightVertex();
                    if (leftVertex.getAction() instanceof SourceAction) {
                        sourceExecutionVertices.add(leftVertex);
                    }
                    inputVerticesMap
                            .computeIfAbsent(rightVertex.getVertexId(), id -> new ArrayList<>())
                            .add(leftVertex);
                    targetVerticesMap
                            .computeIfAbsent(leftVertex.getVertexId(), id -> new ArrayList<>())
                            .add(rightVertex);
                });

        Map<Long, ExecutionVertex> transformChainVertexMap = new HashMap<>();
        Map<Long, Long> chainedTransformVerticesMapping = new HashMap<>();
        for (ExecutionVertex sourceVertex : sourceExecutionVertices) {
            List<ExecutionVertex> vertices = new ArrayList<>();
            vertices.add(sourceVertex);
            for (int index = 0; index < vertices.size(); index++) {
                ExecutionVertex vertex = vertices.get(index);

                fillChainedTransformExecutionVertex(
                        vertex,
                        chainedTransformVerticesMapping,
                        transformChainVertexMap,
                        executionEdges,
                        Collections.unmodifiableMap(inputVerticesMap),
                        Collections.unmodifiableMap(targetVerticesMap));

                if (targetVerticesMap.containsKey(vertex.getVertexId())) {
                    vertices.addAll(targetVerticesMap.get(vertex.getVertexId()));
                }
            }
        }

        Set<ExecutionEdge> transformChainEdges = new LinkedHashSet<>();
        for (ExecutionEdge executionEdge : executionEdges) {
            ExecutionVertex leftVertex = executionEdge.getLeftVertex();
            ExecutionVertex rightVertex = executionEdge.getRightVertex();
            boolean needRebuild = false;
            if (chainedTransformVerticesMapping.containsKey(leftVertex.getVertexId())) {
                needRebuild = true;
                leftVertex =
                        transformChainVertexMap.get(
                                chainedTransformVerticesMapping.get(leftVertex.getVertexId()));
            }
            if (chainedTransformVerticesMapping.containsKey(rightVertex.getVertexId())) {
                needRebuild = true;
                rightVertex =
                        transformChainVertexMap.get(
                                chainedTransformVerticesMapping.get(rightVertex.getVertexId()));
            }
            if (needRebuild) {
                executionEdge = new ExecutionEdge(leftVertex, rightVertex);
            }
            transformChainEdges.add(executionEdge);
        }
        return transformChainEdges;
    }

    private void fillChainedTransformExecutionVertex(
            ExecutionVertex currentVertex,
            Map<Long, Long> chainedTransformVerticesMapping,
            Map<Long, ExecutionVertex> transformChainVertexMap,
            Set<ExecutionEdge> executionEdges,
            Map<Long, List<ExecutionVertex>> inputVerticesMap,
            Map<Long, List<ExecutionVertex>> targetVerticesMap) {
        if (chainedTransformVerticesMapping.containsKey(currentVertex.getVertexId())) {
            return;
        }

        List<ExecutionVertex> transformChainedVertices = new ArrayList<>();
        collectChainedVertices(
                currentVertex,
                transformChainedVertices,
                executionEdges,
                inputVerticesMap,
                targetVerticesMap);
        if (transformChainedVertices.size() > 0) {
            long newVertexId = idGenerator.getNextId();
            List<SeaTunnelTransform> transforms = new ArrayList<>(transformChainedVertices.size());
            List<String> names = new ArrayList<>(transformChainedVertices.size());
            Set<URL> jars = new HashSet<>();

            transformChainedVertices.stream()
                    .peek(
                            vertex ->
                                    chainedTransformVerticesMapping.put(
                                            vertex.getVertexId(), newVertexId))
                    .map(ExecutionVertex::getAction)
                    .map(action -> (TransformAction) action)
                    .forEach(
                            action -> {
                                transforms.add(action.getTransform());
                                jars.addAll(action.getJarUrls());
                                names.add(action.getName());
                            });
            String transformChainActionName =
                    String.format("TransformChain[%s]", String.join("->", names));
            TransformChainAction transformChainAction =
                    new TransformChainAction(
                            newVertexId, transformChainActionName, jars, transforms);
            transformChainAction.setParallelism(currentVertex.getAction().getParallelism());

            ExecutionVertex executionVertex =
                    new ExecutionVertex(
                            newVertexId, transformChainAction, currentVertex.getParallelism());
            transformChainVertexMap.put(newVertexId, executionVertex);
            chainedTransformVerticesMapping.put(
                    currentVertex.getVertexId(), executionVertex.getVertexId());
        }
    }

    private void collectChainedVertices(
            ExecutionVertex currentVertex,
            List<ExecutionVertex> chainedVertices,
            Set<ExecutionEdge> executionEdges,
            Map<Long, List<ExecutionVertex>> inputVerticesMap,
            Map<Long, List<ExecutionVertex>> targetVerticesMap) {
        Action action = currentVertex.getAction();
        // Currently only support Transform action chaining.
        if (action instanceof TransformAction) {
            if (chainedVertices.size() == 0) {
                chainedVertices.add(currentVertex);
            } else if (inputVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
                // It cannot be chained to any input vertex if it has multiple input vertices.
                executionEdges.remove(
                        new ExecutionEdge(
                                chainedVertices.get(chainedVertices.size() - 1), currentVertex));
                chainedVertices.add(currentVertex);
            } else {
                return;
            }
        } else {
            return;
        }

        // It cannot chain to any target vertex if it has multiple target vertices.
        if (targetVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
            collectChainedVertices(
                    targetVerticesMap.get(currentVertex.getVertexId()).get(0),
                    chainedVertices,
                    executionEdges,
                    inputVerticesMap,
                    targetVerticesMap);
        }
    }

    private List<Pipeline> generatePipelines(Set<ExecutionEdge> executionEdges) {
        // 1 获取所有的 ExecutionVertex
        Set<ExecutionVertex> executionVertices = new LinkedHashSet<>();
        for (ExecutionEdge edge : executionEdges) {
            executionVertices.add(edge.getLeftVertex());
            executionVertices.add(edge.getRightVertex());
        }

        // 2 创建 Pipeline 生成器 PipelineGenerator
        PipelineGenerator pipelineGenerator =
                new PipelineGenerator(executionVertices, new ArrayList<>(executionEdges));

        // 3 将 executionEdges 转换为 Pipeline
        List<Pipeline> pipelines = pipelineGenerator.generatePipelines();

        long actionCount = 0;
        Set<String> actionNames = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            Integer pipelineId = pipeline.getId();
            for (ExecutionVertex vertex : pipeline.getVertexes().values()) {
                Action action = vertex.getAction();
                String actionName = String.format("pipeline-%s [%s]", pipelineId, action.getName());
                action.setName(actionName);
                actionNames.add(actionName);
                actionCount++;
            }
        }
        checkArgument(actionNames.size() == actionCount, "Action name is duplicated");

        return pipelines;
    }
}
