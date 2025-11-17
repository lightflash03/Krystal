package com.flipkart.krystal.vajram.graphql.api;

import static com.flipkart.krystal.core.VajramID.vajramID;
import static java.util.Objects.requireNonNull;

import com.flipkart.krystal.core.VajramID;
import com.flipkart.krystal.facets.Dependency;
import com.flipkart.krystal.krystex.kryon.DependentChain;
import com.flipkart.krystal.vajram.facets.specs.DependencySpec;
import com.flipkart.krystal.vajramexecutor.krystex.VajramKryonGraph;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import graphql.com.google.common.collect.ImmutableMap;
import graphql.execution.ExecutionContext;
import graphql.normalized.ExecutableNormalizedField;
import graphql.normalized.ExecutableNormalizedOperation;
import graphql.schema.*;
import java.util.*;

public class QueryAnalyseUtil {

  private static final String FIRST_NODE = "QueryGraphQLAggregator";
  public static final String DATA_FETCHER = "dataFetcher";
  public static final String VAJRAM_ID = "vajramId";
  public static final String REF_FETCHER = "idFetcher";
  public static final String ENTITY_FETCHER = "entityFetcher";

  public static final String GRAPHQL_AGGREGATOR = "GraphQLAggregator";

  static ImmutableSet<DependentChain> getNodeExecutionConfigBasedOnQuery(
      ExecutionContext executionContext,
      Map<GraphQLTypeName, Map<GraphQlFieldSpec, ClassName>> entityTypeToFieldToTypeAggregator,
      Map<GraphQLTypeName, Map<Fetcher, List<GraphQlFieldSpec>>> entityTypeToFetcherToFields,
      VajramKryonGraph vajramKryonGraph) {

    GraphQLSchema graphQLSchema = executionContext.getGraphQLSchema();
    ExecutableNormalizedOperation executableNormalizedOperation =
        executionContext.getNormalizedQueryTree().get();
    ImmutableMap<FieldCoordinates, Collection<ExecutableNormalizedField>> queriedFields =
        executableNormalizedOperation.getCoordinatesToNormalizedFields().asMap();
    Set<VajramID> vajramsToExecute = new HashSet<>();
    Set<DependentChain> dependentChainToSkip = new HashSet<>();
    GraphQLTypeName queriedEntity =
        GraphQLTypeName.of(
            executionContext.getGraphQLSchema().getQueryType().getName().toUpperCase());

    for (FieldCoordinates entry : queriedFields.keySet()) {
      GraphQLAppliedDirective graphQLAppliedDirective =
          graphQLSchema.getFieldDefinition(entry).getAppliedDirective(DATA_FETCHER);

      if (graphQLAppliedDirective != null) {

        vajramsToExecute.add(
            vajramID(graphQLAppliedDirective.getArgument(VAJRAM_ID).getValue().toString()));
      }
      GraphQLAppliedDirective graphQLAppliedDirectiveRef =
          graphQLSchema.getFieldDefinition(entry).getAppliedDirective(REF_FETCHER);
      if (graphQLAppliedDirectiveRef != null) {
        vajramsToExecute.add(
            vajramID(graphQLAppliedDirectiveRef.getArgument(ENTITY_FETCHER).getValue().toString()));
      }
    }

    setSkipDependentChains(
        vajramsToExecute,
        dependentChainToSkip,
        vajramKryonGraph,
        queriedEntity,
        entityTypeToFieldToTypeAggregator,
        entityTypeToFetcherToFields);
    return ImmutableSet.copyOf(dependentChainToSkip);
  }

  private static void setSkipDependentChains(
      Set<VajramID> vajramsToExecute,
      Set<DependentChain> dependentChainToSkip,
      VajramKryonGraph vajramNodeGraph,
      GraphQLTypeName queriedEntity,
      Map<GraphQLTypeName, Map<GraphQlFieldSpec, ClassName>> entityTypeToFieldToTypeAggregator,
      Map<GraphQLTypeName, Map<Fetcher, List<GraphQlFieldSpec>>> entityTypeToFetcherToFields) {

    var facetsByName = vajramNodeGraph.getVajramDefinition(vajramID(FIRST_NODE)).facetsByName();

    for (Map.Entry<GraphQLTypeName, Map<Fetcher, List<GraphQlFieldSpec>>> entityEntry : entityTypeToFetcherToFields.entrySet()) {
      GraphQLTypeName entityType = entityEntry.getKey();

      if (queriedEntity.value().equalsIgnoreCase(entityType.value())) {
        List<Dependency> depList = new ArrayList<>();
        depList.add((Dependency) facetsByName.get(queriedEntity.value().toLowerCase()));
        setSkipDependentChainPerEntity(
            vajramsToExecute,
            dependentChainToSkip,
            vajramNodeGraph,
            entityType,
            depList,
            entityTypeToFieldToTypeAggregator,
            entityTypeToFetcherToFields);
      } else {
        for (Fetcher fetcher : entityEntry.getValue().keySet()) {
          if (fetcher instanceof VajramFetcher vajramFetcher) {
            DependentChain dependentChain =
                vajramNodeGraph.computeDependentChain(
                    FIRST_NODE,
                    (Dependency)
                        requireNonNull(facetsByName.get(entityType.value().toLowerCase())),
                    (Dependency) requireNonNull(facetsByName.get(vajramFetcher.vajramClassName().simpleName())));
            dependentChainToSkip.add(dependentChain);
          }
        }
      }
    }
  }

  private static void setSkipDependentChainPerEntity(
      Set<VajramID> vajramsToExecute,
      Set<DependentChain> dependentChainToSkip,
      VajramKryonGraph vajramNodeGraph,
      GraphQLTypeName entityType,
      List<Dependency> dependencyList,
      Map<GraphQLTypeName, Map<GraphQlFieldSpec, ClassName>> entityTypeToFieldToTypeAggregator,
      Map<GraphQLTypeName, Map<Fetcher, List<GraphQlFieldSpec>>> entityTypeToFetcherToFields) {

    Map<Fetcher, List<GraphQlFieldSpec>> fetcherToFieldsMap = entityTypeToFetcherToFields.get(entityType);

    if (fetcherToFieldsMap == null) {
      return;
    }

    // Process each fetcher (vajram) for this entity
    for (Fetcher fetcher : fetcherToFieldsMap.keySet()) {
      if (!(fetcher instanceof VajramFetcher vajramFetcher)) {
        continue;
      }
      String vajramName = vajramFetcher.vajramClassName().simpleName();

      if (!vajramsToExecute.contains(vajramID(vajramName))) {
        Dependency mostRecentDependency = null;
        Dependency[] depChain = new Dependency[dependencyList.size()];
        if (dependencyList.size() > 1) {
          for (int i = 1; i < dependencyList.size(); i++) {
            mostRecentDependency = dependencyList.get(i);
            depChain[i - 1] = mostRecentDependency;
          }
        }
        if (mostRecentDependency instanceof DependencySpec<?, ?, ?> dependencySpec) {
          VajramID mostRecentVajram = dependencySpec.onVajramID();
          var facetsByName = vajramNodeGraph.getVajramDefinition(mostRecentVajram).facetsByName();
          depChain[dependencyList.size() - 1] = (Dependency) facetsByName.get(vajramName);
          DependentChain dependentChain =
              vajramNodeGraph.computeDependentChain(FIRST_NODE, dependencyList.get(0), depChain);
          dependentChainToSkip.add(dependentChain);
        } else {
          throw new UnsupportedOperationException(
              "Unknown dependency type: " + mostRecentDependency);
        }
      }
    }
    if (!dependencyList.isEmpty()) {
      Dependency mostRecentDependency = dependencyList.get(dependencyList.size() - 1);
      if (mostRecentDependency instanceof DependencySpec<?, ?, ?> dependencySpec) {
        VajramID mostRecentVajram = dependencySpec.onVajramID();
        var facetsByName = vajramNodeGraph.getVajramDefinition(mostRecentVajram).facetsByName();

        Map<GraphQlFieldSpec, ClassName> fieldToTypeAggregatorMap = entityTypeToFieldToTypeAggregator.get(entityType);

        if (fieldToTypeAggregatorMap != null) {
          for (Map.Entry<GraphQlFieldSpec, ClassName> refFieldEntry : fieldToTypeAggregatorMap.entrySet()) {
            GraphQlFieldSpec refField = refFieldEntry.getKey();
            ClassName targetAggregatorClass = refFieldEntry.getValue();

            // Extract target entity name from aggregator class name
            // e.g., "DummyGraphQLAggregator" -> "DUMMY"
            String aggregatorName = targetAggregatorClass.simpleName();
            String targetEntityName = aggregatorName.substring(0, aggregatorName.length() - GRAPHQL_AGGREGATOR.length());
            GraphQLTypeName targetEntityType = new GraphQLTypeName(targetEntityName.toUpperCase());

            // Build new dependency list for recursive call
            List<Dependency> newDepList = new ArrayList<>(dependencyList);
            newDepList.add((Dependency) facetsByName.get(refField.fieldName()));

            setSkipDependentChainPerEntity(
                vajramsToExecute,
                dependentChainToSkip,
                vajramNodeGraph,
                targetEntityType,
                newDepList,
                entityTypeToFieldToTypeAggregator,
                entityTypeToFetcherToFields);
          }
        }
      }
    } else {
      throw new IllegalStateException("Empty dependency not yet handled");
    }
  }
}
