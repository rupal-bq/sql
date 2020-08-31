/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.planner.physical;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTupleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.storage.bindingtuple.BindingTuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Group the all the input {@link BindingTuple} by {@link RareTopNOperator#groupByExprList},
 * Calculate the rare result by using the {@link RareTopNOperator#fieldExprList}.
 */
@ToString
@EqualsAndHashCode
public class RareTopNOperator extends PhysicalPlan {

  @Getter
  private final PhysicalPlan input;

  /**
   * If rareTopFlag is true, result for top will be returned.
   * If rareTopFlag is false, result for rare will be returned.
   */
  @Getter
  private final Boolean rareTopFlag;
  @Getter
  private final Integer noOfResults;
  @Getter
  private final List<Expression> fieldExprList;
  @Getter
  private final List<Expression> groupByExprList;

  @EqualsAndHashCode.Exclude
  private final Group group;
  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  private static final Integer DEFAULT_NO_OF_RESULTS = 10;


  public RareTopNOperator(PhysicalPlan input, Boolean rareTopFlag, List<Expression> fieldExprList,
      List<Expression> groupByExprList) {
    this(input, rareTopFlag, DEFAULT_NO_OF_RESULTS, fieldExprList, groupByExprList);
  }

  /**
   * RareTopNOperator Constructor.
   *
   * @param input           Input {@link PhysicalPlan}
   * @param rareTopFlag     Flag for Rare/TopN command.
   * @param noOfResults     Number of results
   * @param fieldExprList   List of {@link Expression}
   * @param groupByExprList List of group by {@link Expression}
   */
  public RareTopNOperator(PhysicalPlan input, Boolean rareTopFlag, int noOfResults,
      List<Expression> fieldExprList,
      List<Expression> groupByExprList) {
    this.input = input;
    this.rareTopFlag = rareTopFlag;
    this.noOfResults = noOfResults;
    this.fieldExprList = fieldExprList;
    this.groupByExprList = groupByExprList;
    this.group = new Group();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRareTopN(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public void open() {
    super.open();
    while (input.hasNext()) {
      group.push(input.next());
    }
    iterator = group.result().iterator();
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class Group {

    private final Map<Key, List<HashMap<Key, Integer>>> groupListMap = new HashMap<>();

    /**
     * Push the BindingTuple to Group.
     */
    public void push(ExprValue inputValue) {
      Key groupKey = new Key(inputValue, groupByExprList);
      Key fieldKey = new Key(inputValue, fieldExprList);
      groupListMap.computeIfAbsent(groupKey, k -> {
        List<HashMap<Key, Integer>> list = new ArrayList<>();
        HashMap<Key, Integer> map = new HashMap<>();
        map.put(fieldKey, 1);
        list.add(map);
        return list;
      });
      groupListMap.computeIfPresent(groupKey, (key, fieldList) -> {
        fieldList.forEach(map -> {
          map.computeIfAbsent(fieldKey, f -> 1);
          map.computeIfPresent(fieldKey, (field, count) -> {
            return count + 1;
          });
        });
        return fieldList;
      });
    }

    /**
     * Get the list of {@link BindingTuple} for each group.
     */
    public List<ExprValue> result() {
      ImmutableList.Builder<ExprValue> resultBuilder = new ImmutableList.Builder<>();

      groupListMap.forEach((groups, list) -> {
        Map<String, ExprValue> map = new LinkedHashMap<>();
        list.forEach(fieldMap -> {
          List<Key> result = find(fieldMap);
          result.forEach(field -> {
            map.putAll(groups.keyMap(groupByExprList));
            map.putAll(field.keyMap(fieldExprList));
            resultBuilder.add(ExprTupleValue.fromExprValueMap(map));
          });
        });
      });

      return resultBuilder.build();
    }

    /**
     * Get a list of result.
     */
    public List<Key> find(HashMap<Key, Integer> map) {
      Comparator<Map.Entry<Key, Integer>> valueComparator;
      if (rareTopFlag) {
        valueComparator = Map.Entry.comparingByValue(Comparator.reverseOrder());
      } else {
        valueComparator = Map.Entry.comparingByValue();
      }

      return map.entrySet().stream().sorted(valueComparator).limit(noOfResults)
          .map(Map.Entry::getKey).collect(Collectors.toList());
    }
  }

  /**
   * Key.
   */
  @EqualsAndHashCode
  @VisibleForTesting
  public class Key {

    private final List<ExprValue> valueList;

    /**
     * Key constructor.
     */
    public Key(ExprValue value, List<Expression> exprList) {
      this.valueList = exprList.stream()
          .map(expr -> expr.valueOf(value.bindingTuples())).collect(Collectors.toList());
    }

    /**
     * Return the Map of key and key value.
     */
    public Map<String, ExprValue> keyMap(List<Expression> exprList) {

      return Streams.zip(
          exprList.stream().map(
              expression -> expression.toString()),
          valueList.stream(),
          AbstractMap.SimpleEntry::new
      ).collect(Collectors.toMap(key -> key.getKey(), key -> key.getValue()));
    }
  }

}
