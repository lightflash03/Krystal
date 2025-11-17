package com.flipkart.krystal.vajram.graphql.api;

import static com.flipkart.krystal.vajram.graphql.api.QueryAnalyseUtil.getNodeExecutionConfigBasedOnQuery;
import static graphql.execution.ExecutionStrategyParameters.newParameters;

import com.flipkart.krystal.data.ImmutableRequest;
import com.flipkart.krystal.krystex.kryon.DependentChain;
import com.flipkart.krystal.krystex.kryon.KryonExecutionConfig;
import com.flipkart.krystal.vajram.graphql.api.ExecutionLifecycleListener.ExecutionStartEvent;
import com.flipkart.krystal.vajramexecutor.krystex.KrystexVajramExecutor;
import com.flipkart.krystal.vajramexecutor.krystex.KrystexVajramExecutorConfig;
import com.flipkart.krystal.vajramexecutor.krystex.KrystexVajramExecutorConfig.KrystexVajramExecutorConfigBuilder;
import com.flipkart.krystal.vajramexecutor.krystex.VajramKryonGraph;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import graphql.ExecutionResult;
import graphql.GraphQLContext;
import graphql.GraphqlErrorException;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ExecutionStrategy;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.ExecutionStrategyParameters.Builder;
import graphql.execution.FieldCollector;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.MergedField;
import graphql.execution.MergedSelectionSet;
import graphql.execution.NonNullableFieldValidator;
import graphql.execution.NonNullableFieldWasNullException;
import graphql.execution.ResultPath;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;

// TODO: remove this once it's moved to graphQlJava, did it due to
//  [array] -> [array] field support in schema resolution support
public class VajramExecutionStrategy extends ExecutionStrategy {

  public static final String VAJRAM_KRYON_GRAPH = "vajram_kryon_graph";
  public static final String VAJRAM_EXECUTOR_CONFIG = "vajram_executor_config";
  public static final String TASK_ID = "taskId";
  public static final String TYPENAME_FIELD = "__typename";
  private final FieldCollector fieldCollector = new FieldCollector();
  private final InitVajramRequestCreator initVajramRequestCreator;
  private final ExecutionLifecycleListener executionLifecycleListener;
  private final Map<GraphQLTypeName, Map<GraphQlFieldSpec, ClassName>> entityTypeToFieldToTypeAggregator;
  private final Map<GraphQLTypeName, Map<Fetcher, List<GraphQlFieldSpec>>> entityTypeToFetcherToFields;

  public VajramExecutionStrategy(
      InitVajramRequestCreator initVajramRequestCreator,
      ExecutionLifecycleListener executionLifecycleListener,
      Map<GraphQLTypeName, Map<GraphQlFieldSpec, ClassName>> entityTypeToFieldToTypeAggregator,
      Map<GraphQLTypeName, Map<Fetcher, List<GraphQlFieldSpec>>> entityTypeToFetcherToFields) {
    this.initVajramRequestCreator = initVajramRequestCreator;
    this.executionLifecycleListener = executionLifecycleListener;
    this.entityTypeToFieldToTypeAggregator = entityTypeToFieldToTypeAggregator;
    this.entityTypeToFetcherToFields = entityTypeToFetcherToFields;
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(
      ExecutionContext executionContext, ExecutionStrategyParameters parameters)
      throws NonNullableFieldWasNullException {
    GraphQLContext graphQLContext = executionContext.getGraphQLContext();
    KrystexVajramExecutorConfigBuilder vajramExecutorConfigBuilder =
        graphQLContext.getOrDefault(VAJRAM_EXECUTOR_CONFIG, KrystexVajramExecutorConfig.builder());
    VajramKryonGraph vajramKryonGraph = graphQLContext.get(VAJRAM_KRYON_GRAPH);

    ImmutableSet<DependentChain> dependantChainList =
        getNodeExecutionConfigBasedOnQuery(
            executionContext,
            entityTypeToFieldToTypeAggregator,
            entityTypeToFetcherToFields,
            vajramKryonGraph);

    ImmutableRequest<?> immutableRequest =
        initVajramRequestCreator.create(executionContext, this, getParams(parameters));
    try (KrystexVajramExecutor vajramExecutor =
        KrystexVajramExecutor.builder()
            .vajramKryonGraph(vajramKryonGraph)
            .executorConfig(vajramExecutorConfigBuilder.build())
            .build()) {
      executionLifecycleListener.onExecutionStart(
          new ExecutionStartEvent(vajramExecutor, executionContext));
      return vajramExecutor
          .execute(
              immutableRequest,
              KryonExecutionConfig.builder()
                  .disabledDependentChains(dependantChainList)
                  .executionId(graphQLContext.get(TASK_ID))
                  .build())
          .handle(
              (o, throwable) -> {
                if (throwable != null) {
                  return ExecutionResult.newExecutionResult()
                      .addError(GraphqlErrorException.newErrorException().cause(throwable).build())
                      .build();
                } else {
                  return ExecutionResult.newExecutionResult().data(o).build();
                }
              });
    }
  }

  private ExecutionStrategyParameters getParams(ExecutionStrategyParameters parameters) {
    MergedSelectionSet fields = parameters.getFields();
    List<String> fieldNames = fields.getKeys();
    Iterator var9 = fieldNames.iterator();
    ExecutionStrategyParameters newParameters = parameters;
    while (var9.hasNext()) {
      String fieldName = (String) var9.next();
      MergedField currentField = fields.getSubField(fieldName);
      ResultPath fieldPath = parameters.getPath().segment(mkNameForPath(currentField));
      newParameters =
          parameters.transform(
              (builder) -> {
                builder.field(currentField).path(fieldPath).parent(parameters);
              });
    }
    return newParameters;
  }

  public ExecutionStrategyParameters newParametersForFieldExecution(
      ExecutionContext executionContext,
      ExecutionStrategyParameters parameters,
      @Nullable MergedField field) {
    ResultPath resultPath = parameters.getPath();
    if (field != null) {
      resultPath = parameters.getPath().segment(mkNameForPath(field));
    }
    Builder builder = newParameters(parameters).path(resultPath);
    if (field != null) {
      builder.field(field);
    }
    ExecutionStepInfo executionStepInfo = parameters.getExecutionStepInfo();
    GraphQLObjectType resolvedObjectType =
        resolveType(executionContext, parameters, executionStepInfo.getUnwrappedNonNullType());
    ExecutionStepInfo newExecutionStepInfo =
        executionStepInfo.changeTypeWithPreservedNonNull(resolvedObjectType);
    NonNullableFieldValidator nonNullableFieldValidator =
        new NonNullableFieldValidator(executionContext);
    builder
        .executionStepInfo(newExecutionStepInfo)
        .nonNullFieldValidator(nonNullableFieldValidator)
        .source(executionContext.getValueUnboxer().unbox(parameters.getSource()))
        .parent(parameters);
    FieldCollectorParameters collectorParameters =
        FieldCollectorParameters.newParameters()
            .schema(executionContext.getGraphQLSchema())
            .objectType(resolvedObjectType)
            .fragments(executionContext.getFragmentsByName())
            .variables(executionContext.getCoercedVariables().toMap())
            .build();

    ExecutionStrategyParameters newParameters = builder.build();
    MergedSelectionSet subFields;

    if (field != null) {
      subFields = fieldCollector.collectFields(collectorParameters, newParameters.getField());
      newParameters =
          newParameters(newParameters)
              .executionStepInfo(
                  createExecutionStepInfo(
                      executionContext,
                      newParameters,
                      getFieldDef(executionContext, newParameters, field.getSingleField()),
                      resolvedObjectType))
              .build();
      return newParameters(newParameters).fields(subFields).build();
    } else {
      return newParameters(newParameters).build();
    }
  }

  /**
   * Copied from {@link ExecutionStrategy#mkNameForPath(MergedField)} as that is {@link
   * graphql.Internal}
   */
  public static String mkNameForPath(MergedField mergedField) {
    return mergedField.getFields().get(0).getResultKey();
  }

  @Override
  protected GraphQLObjectType resolveType(
      ExecutionContext executionContext,
      ExecutionStrategyParameters parameters,
      GraphQLType fieldType) {
    if (fieldType instanceof GraphQLList graphQLList) {
      fieldType = graphQLList.getWrappedType();
    }
    return super.resolveType(executionContext, parameters, fieldType);
  }

  @FunctionalInterface
  public interface InitVajramRequestCreator {
    ImmutableRequest<?> create(
        ExecutionContext graphQLExecutionContext,
        VajramExecutionStrategy graphQLExecutionStrategy,
        ExecutionStrategyParameters graphQLExecutionStrategyParameters);
  }
}
