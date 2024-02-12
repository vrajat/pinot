package org.apache.pinot.controller.helix.core.restream;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


@ApiModel
public class TableRestreamConfig {
  @JsonProperty("destTableName")
  @ApiModelProperty(example = "false")
  private String _destTableName;

  @JsonProperty("tenant")
  @ApiModelProperty(example = "false")
  private String _tenant;

  public String getDestTableName() {
    return _destTableName;
  }

  public void setDestTableName(String destTableName) {
    _destTableName = destTableName;
  }

  public String getTenant() {
    return _tenant;
  }

  public void setTenant(String tenant) {
    _tenant = tenant;
  }
}
