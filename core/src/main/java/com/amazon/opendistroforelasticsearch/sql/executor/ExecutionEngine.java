/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.executor;

import com.amazon.opendistroforelasticsearch.sql.common.response.ResponseListener;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import java.util.List;
import lombok.Data;

/**
 * Execution engine that encapsulates execution details.
 */
public interface ExecutionEngine {

  /**
   * Execute physical plan and call back response listener.
   *
   * @param plan     executable physical plan
   * @param listener response listener
   */
  void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener);

  /**
   * Data class that encapsulates ExprValue.
   */
  @Data
  class QueryResponse {
    private final List<ExprValue> results;
  }

}
