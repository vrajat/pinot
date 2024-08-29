package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.cursors.QueryStore;
import org.apache.pinot.spi.cursors.ResultMetadata;
import org.apache.pinot.spi.cursors.ResultStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "ResultStore", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
    description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")))
@Path("/resultStore")
public class ResultStoreResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultStoreResource.class);

  @Inject
  private BrokerMetrics _brokerMetrics;

  @Inject
  private ResultStore _resultStore;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{requestId}/metadata")
  @ApiOperation(value = "ResultStore metadata for a query")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public JsonNode getSqlQueryMetadata(
      @ApiParam(value = "Request ID of the query", required = true) @PathParam("requestId") String requestId) {
    try {
      QueryStore queryStore = _resultStore.getQueryStore(requestId);
      if (queryStore != null) {
        return queryStore.getQueryResponseMetadata();
      } else {
        throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
            .entity(String.format("Query results for %s not found.", requestId)).build());
      }
    } catch (WebApplicationException wae) {
      throw wae;
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1L);
      throw new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }
  }

  public static class ResultResponse {
    private final List<ResultMetadata> _resultMetadataList;

    public ResultResponse(@JsonProperty("resultMetadataList") List<ResultMetadata> resultMetadataList) {
      _resultMetadataList = resultMetadataList;
    }

    @JsonProperty("resultMetadataList")
    public List<ResultMetadata> getResultMetadataList() {
      return _resultMetadataList;
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_RESULT_STORE)
  @ApiOperation(value = "Get metadata of all query stores in the result store", notes = "Get metadata of all "
      + "query stores in the result store")
  public ResultResponse getResults(@Context HttpHeaders headers) {
    try {
      Collection<? extends QueryStore> queryStores = _resultStore.getAllQueryStores();
      List<ResultMetadata> metadataList = new ArrayList<>(queryStores.size());
      for (QueryStore queryStore : queryStores) {
        metadataList.add(queryStore.getResultMetadata());
      }
      return new ResultResponse(metadataList);
    } catch (Exception e) {
      throw new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{requestId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_RESULT_STORE)
  @ApiOperation(value = "Delete a query store in the result store", notes = "Delete a query store in the result store")
  public String deleteResult(
      @ApiParam(value = "Request ID of the query", required = true) @PathParam("requestId") String requestId,
      @Context HttpHeaders headers) {
    try {
      QueryStore queryStore = _resultStore.deleteQueryStore(requestId);
      if (queryStore != null) {
        return "Query Results for " + requestId + " deleted.";
      }
    } catch (Exception e) {
      throw new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }

    // Query Result not found. Throw error.
    throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND).entity(String.format("Query results for %s not found.", requestId))
            .build());
  }
}
