/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.expression.datetime;

import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.getStringValue;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DATE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DATETIME;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.TIME;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName.DAYOFMONTH;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionDSL.define;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionDSL.impl;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDateValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprIntegerValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprStringValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimeValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimestampValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionRepository;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionResolver;
import java.util.Calendar;
import lombok.experimental.UtilityClass;

/**
 * The definition of date and time functions.
 * 1) have the clear interface for function define.
 * 2) the implementation should rely on ExprValue.
 */
@UtilityClass
public class DateTimeFunction {
  /**
   * Register Date and Time Functions.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(date());
    repository.register(dayOfMonth());
    repository.register(time());
    repository.register(timestamp());
    repository.register(week());
  }

  /**
   * Extracts the date part of a date and time value.
   * Also to construct a date type. The supported signatures:
   * STRING/DATE/DATETIME/TIMESTAMP -> DATE
   */
  private FunctionResolver date() {
    return define(BuiltinFunctionName.DATE.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprDate), DATE, TIMESTAMP));
  }

  /**
   * DAYOFMONTH(DATE). return the day of the month (1-31).
   */
  private FunctionResolver dayOfMonth() {
    return define(DAYOFMONTH.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth),
            INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprDayOfMonth), INTEGER, STRING)
    );
  }

  /**
   * Extracts the time part of a date and time value.
   * Also to construct a time type. The supported signatures:
   * STRING/DATE/DATETIME/TIME/TIMESTAMP -> TIME
   */
  private FunctionResolver time() {
    return define(BuiltinFunctionName.TIME.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, TIME),
        impl(nullMissingHandling(DateTimeFunction::exprTime), TIME, TIMESTAMP));
  }

  /**
   * Extracts the timestamp of a date and time value.
   * Also to construct a date type. The supported signatures:
   * STRING/DATE/DATETIME/TIMESTAMP -> DATE
   */
  private FunctionResolver timestamp() {
    return define(BuiltinFunctionName.TIMESTAMP.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, STRING),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, DATETIME),
        impl(nullMissingHandling(DateTimeFunction::exprTimestamp), TIMESTAMP, TIMESTAMP));
  }

  /**
   * WEEK(DATE[,mode]). return the week number for date.
   */
  private FunctionResolver week() {
    return define(BuiltinFunctionName.WEEK.getName(),
        impl(nullMissingHandling(DateTimeFunction::exprWeekWithoutMode), INTEGER, DATE),
        impl(nullMissingHandling(DateTimeFunction::exprWeek), INTEGER, DATE, INTEGER)
    );
  }

    /**
     * Date implementation for ExprValue.
     * @param exprValue ExprValue of Date type or String type.
     * @return ExprValue.
     */
  private ExprValue exprDate(ExprValue exprValue) {
    if (exprValue instanceof ExprStringValue) {
      return new ExprDateValue(exprValue.stringValue());
    } else {
      return new ExprDateValue(exprValue.dateValue());
    }
  }

  /**
   * Day of Month implementation for ExprValue.
   * @param date ExprValue of Date type.
   * @return ExprValue.
   */
  private ExprValue exprDayOfMonth(ExprValue date) {
    if (date instanceof ExprStringValue) {
      return new ExprIntegerValue(
          new ExprDateValue(date.stringValue()).dateValue().getDayOfMonth());
    }
    return new ExprIntegerValue(date.dateValue().getDayOfMonth());
  }

  /**
   * Time implementation for ExprValue.
   * @param exprValue ExprValue of Time type or String.
   * @return ExprValue.
   */
  private ExprValue exprTime(ExprValue exprValue) {
    if (exprValue instanceof ExprStringValue) {
      return new ExprTimeValue(exprValue.stringValue());
    } else {
      return new ExprTimeValue(exprValue.timeValue());
    }
  }

  /**
   * Timestamp implementation for ExprValue.
   * @param exprValue ExprValue of Timestamp type or String type.
   * @return ExprValue.
   */
  private ExprValue exprTimestamp(ExprValue exprValue) {
    if (exprValue instanceof ExprStringValue) {
      return new ExprTimestampValue(exprValue.stringValue());
    } else {
      return new ExprTimestampValue(exprValue.timestampValue());
    }
  }

  /**
   * Week for date implementation for ExprValue.
   * @param date ExprValue of Date type.
   * @return ExprValue.
   */
  private ExprValue exprWeekWithoutMode(ExprValue date) {
    return exprWeek(date, new ExprIntegerValue(0));
  }

  private ExprValue exprWeek(ExprValue date, ExprValue mode) {
    Calendar calendar = Calendar.getInstance();
    switch (mode.integerValue()) {
      case 0:
      case 2:
        calendar.setFirstDayOfWeek(Calendar.SUNDAY);
        calendar.setMinimalDaysInFirstWeek(7);
        break;
      case 1:
      case 3:
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setMinimalDaysInFirstWeek(5);
        break;
      case 4:
      case 6:
        calendar.setFirstDayOfWeek(Calendar.SUNDAY);
        calendar.setMinimalDaysInFirstWeek(4);
        break;
      case 5:
      case 7:
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setMinimalDaysInFirstWeek(6);
        break;
    }

    calendar.set(date.dateValue().getYear(), date.dateValue().getMonthValue()-1, date.dateValue().getDayOfMonth());
    int weekNumber = calendar.get(Calendar.WEEK_OF_YEAR);

    if(weekNumber == 0){
      calendar.set(date.dateValue().getYear() - 1, 12, 31);
      weekNumber = calendar.get(Calendar.WEEK_OF_YEAR);
    }

    return new ExprIntegerValue(weekNumber);
  }

}
