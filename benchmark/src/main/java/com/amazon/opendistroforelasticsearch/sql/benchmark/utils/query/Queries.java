/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.benchmark.utils.query;

import java.util.LinkedList;

public class Queries {

  public static int tpchQueriesCountMax = 22;
  public static LinkedList<String> queries = new LinkedList<>();

  static {
    // TODO: Add proper queries supported by all databases.
    queries.add("CREATE OR REPLACE FUNCTION  CASSANDRA_EXAMPLE_KEYSPACE.fSumDiscPrice "
        + "(l_extendedprice double,l_discount double) CALLED ON NULL INPUT RETURNS double LANGUAGE "
        + "java AS 'return (Double.valueOf( l_extendedprice.doubleValue() * "
        + " (1.0 - l_discount.doubleValue() ) ));");
    queries.add("CREATE OR REPLACE FUNCTION CASSANDRA_EXAMPLE_KEYSPACE.fSumChargePrice "
        + "(l_extendedprice double,l_discount double,l_tax double) CALLED ON NULL INPUT RETURNS "
        + "double LANGUAGE java AS 'return (Double.valueOf( l_extendedprice.doubleValue() *  "
        + "(1.0 - l_discount.doubleValue() ) * (1.0 + l_tax.doubleValue()) ));';");
    queries.add("SELECT "
        + " returnflag, "
        + " linestatus, "
        + " sum(quantity) as sum_qty, "
        + " sum(extendedprice) as sum_base_price, "
        + " sum(CASSANDRA_EXAMPLE_KEYSPACE.fSumDiscPrice(extendedprice,discount))"
        + " as sum_disc_price, "
        + " sum(CASSANDRA_EXAMPLE_KEYSPACE.fSumChargePrice(extendedprice,discount,tax))"
        + " as sum_charge, "
        + " avg(quantity) as avg_qty, avg(extendedprice) as avg_price, "
        + " avg(discount) as avg_disc, "
        + " count(*) as count_order "
        + "FROM "
        + " CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q1 "
        + "WHERE "
        + " shipdate < '2000-01-01 22:00:00-0700' "
        + " and returnflag='N' "
        + " and linestatus = 'O' ;");
    ;
  }
}
