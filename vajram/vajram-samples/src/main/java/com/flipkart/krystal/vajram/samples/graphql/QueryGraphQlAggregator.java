package com.flipkart.krystal.vajram.samples.graphql;

import com.flipkart.krystal.vajram.ComputeVajram;
import com.flipkart.krystal.vajram.Dependency;
import com.flipkart.krystal.vajram.Output;
import com.flipkart.krystal.vajram.facets.MultiExecute;
import com.flipkart.krystal.vajram.facets.resolution.sdk.Resolve;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@SuppressWarnings("OptionalContainsCollection")
abstract class QueryGraphQlAggregator extends ComputeVajram<Object> {
  @SuppressWarnings("OptionalContainsCollection")
  static class _Facets {

    @Dependency(onVajram = GetFilteredPaginatedShipmentIds.class)
    Optional<Set<String>> vg_shipment_refs;

    @Dependency(onVajram = ShipmentGraphqlAggregator.class, canFanout = true)
    Optional<List<Shipment>> shipments;

    @Dependency(onVajram = OrderGraphqlAggregator.class)
    Optional<Order> order;
  }

  @Resolve(depName = "order", depInputs = "orderId")
  String orderIdForOrder(ExecutionContext context) {
    String orderID = context.rawVariables().get("orderID");
    return orderID;
  }

  @Resolve(depName = "shipments", depInputs = "shipmentId")
  MultiExecute<String> shipmentIdsForShipments(
      Optional<Set<String>> vg_shipment_refs) {
    return MultiExecute.executeFanoutWith(
        vg_shipment_refs.orElseThrow(
            () -> new IllegalArgumentException("Shipent Id Fetch Failed")));
  }

  @Output
  static output(){

  }
}
