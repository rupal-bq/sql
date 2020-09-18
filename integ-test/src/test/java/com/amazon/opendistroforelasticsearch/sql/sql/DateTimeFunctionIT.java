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

package com.amazon.opendistroforelasticsearch.sql.sql;

import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.rows;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.schema;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.verifyDataRows;
import static com.amazon.opendistroforelasticsearch.sql.util.MatcherUtils.verifySchema;
import static com.amazon.opendistroforelasticsearch.sql.util.TestUtils.getResponseBody;

import com.amazon.opendistroforelasticsearch.sql.legacy.SQLIntegTestCase;
import com.amazon.opendistroforelasticsearch.sql.util.TestUtils;
import java.io.IOException;
import java.util.Locale;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class DateTimeFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    TestUtils.enableNewQueryEngine(client());
  }

  @Test
  public void testDateAdd() throws IOException {
    JSONObject result =
        executeQuery("select date_add(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("date_add(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 17:30:00"));

    result = executeQuery("select date_add(date('2020-09-16'), 1)");
    verifySchema(result, schema("date_add(date('2020-09-16'), 1)", null, "date"));
    verifyDataRows(result, rows("2020-09-17"));
  }

  @Test
  public void testDateSub() throws IOException {
    JSONObject result =
        executeQuery("select date_sub(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("date_sub(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select date_sub(date('2020-09-16'), 1)");
    verifySchema(result, schema("date_sub(date('2020-09-16'), 1)", null, "date"));
    verifyDataRows(result, rows("2020-09-15"));
  }

  @Test
  public void testDay() throws IOException {
    JSONObject result = executeQuery("select day(date('2020-09-16'))");
    verifySchema(result, schema("day(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(16));
  }

  @Test
  public void testDayName() throws IOException {
    JSONObject result = executeQuery("select dayname(date('2020-09-16'))");
    verifySchema(result, schema("dayname(date('2020-09-16'))", null, "string"));
    verifyDataRows(result, rows("Wednesday"));
  }

  @Test
  public void testDayOfMonth() throws IOException {
    JSONObject result = executeQuery("select dayofmonth(date('2020-09-16'))");
    verifySchema(result, schema("dayofmonth(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(16));
  }

  @Test
  public void testDayOfWeek() throws IOException {
    JSONObject result = executeQuery("select dayofweek(date('2020-09-16'))");
    verifySchema(result, schema("dayofweek(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(4));
  }

  @Test
  public void testDayOfYear() throws IOException {
    JSONObject result = executeQuery("select dayofyear(date('2020-09-16'))");
    verifySchema(result, schema("dayofyear(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(260));
  }

  @Test
  public void testFromDays() throws IOException {
    JSONObject result = executeQuery("select from_days(738049)");
    verifySchema(result, schema("from_days(738049)", null, "date"));
    verifyDataRows(result, rows("2020-09-16"));
  }

  @Test
  public void testHour() throws IOException {
    JSONObject result = executeQuery("select hour(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("hour(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour(time('17:30:00'))");
    verifySchema(result, schema("hour(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));
  }

  @Test
  public void testMicrosecond() throws IOException {
    JSONObject result = executeQuery("select microsecond(timestamp('2020-09-16 17:30:00.123456'))");
    verifySchema(result, schema("microsecond(timestamp('2020-09-16 17:30:00.123456'))", null, "integer"));
    verifyDataRows(result, rows(123456));

    result = executeQuery("select microsecond(time('17:30:00.000010'))");
    verifySchema(result, schema("microsecond(time('17:30:00.000010'))", null, "integer"));
    verifyDataRows(result, rows(10));
  }

  @Test
  public void testMinute() throws IOException {
    JSONObject result = executeQuery("select minute(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("minute(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute(time('17:30:00'))");
    verifySchema(result, schema("minute(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(30));
  }

  @Test
  public void testMonth() throws IOException {
    JSONObject result = executeQuery("select month(date('2020-09-16'))");
    verifySchema(result, schema("month(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(9));
  }

  @Test
  public void testMonthName() throws IOException {
    JSONObject result = executeQuery("select monthname(date('2020-09-16'))");
    verifySchema(result, schema("monthname(date('2020-09-16'))", null, "string"));
    verifyDataRows(result, rows("September"));
  }

  @Test
  public void testQuarter() throws IOException {
    JSONObject result = executeQuery("select quarter(date('2020-09-16'))");
    verifySchema(result, schema("quarter(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(3));
  }

  @Test
  public void testSecond() throws IOException {
    JSONObject result = executeQuery("select second(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("second(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second(time('17:30:00'))");
    verifySchema(result, schema("second(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(0));
  }

  @Test
  public void testSubDate() throws IOException {
    JSONObject result =
        executeQuery("select subdate(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("subdate(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select subdate(date('2020-09-16'), 1)");
    verifySchema(result, schema("subdate(date('2020-09-16'), 1)", null, "date"));
    verifyDataRows(result, rows("2020-09-15"));
  }

  @Test
  public void testTimeToSec() throws IOException {
    JSONObject result = executeQuery("select time_to_sec(time('17:30:00'))");
    verifySchema(result, schema("time_to_sec(time('17:30:00'))", null, "long"));
    verifyDataRows(result, rows(63000));
  }

  @Test
  public void testToDays() throws IOException {
    JSONObject result = executeQuery("select to_days(date('2020-09-16'))");
    verifySchema(result, schema("to_days(date('2020-09-16'))", null, "long"));
    verifyDataRows(result, rows(738049));
  }

  @Test
  public void testYear() throws IOException {
    JSONObject result = executeQuery("select year(date('2020-09-16'))");
    verifySchema(result, schema("year(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(2020));
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}