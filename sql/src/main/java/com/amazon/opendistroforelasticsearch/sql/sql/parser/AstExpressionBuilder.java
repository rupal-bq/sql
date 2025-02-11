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

package com.amazon.opendistroforelasticsearch.sql.sql.parser;

import static com.amazon.opendistroforelasticsearch.sql.common.utils.StringUtils.unquoteIdentifier;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.BooleanContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.MathExpressionAtomContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.ScalarFunctionCallContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.SignedDecimalContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.SignedRealContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.StringContext;

import com.amazon.opendistroforelasticsearch.sql.ast.dsl.AstDSL;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Function;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.UnresolvedExpression;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParser.NestedExpressionAtomContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenDistroSQLParserBaseVisitor;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Expression builder to parse text to expression in AST.
 */
public class AstExpressionBuilder extends OpenDistroSQLParserBaseVisitor<UnresolvedExpression> {

  @Override
  public UnresolvedExpression visitMathExpressionAtom(MathExpressionAtomContext ctx) {
    return new Function(
        ctx.mathOperator().getText(),
        Arrays.asList(visit(ctx.left), visit(ctx.right))
    );
  }

  @Override
  public UnresolvedExpression visitNestedExpressionAtom(NestedExpressionAtomContext ctx) {
    return visit(ctx.expression()); // Discard parenthesis around
  }

  @Override
  public UnresolvedExpression visitScalarFunctionCall(ScalarFunctionCallContext ctx) {
    return new Function(
        ctx.scalarFunctionName().getText(),
        ctx.functionArgs()
           .functionArg()
           .stream()
           .map(this::visitFunctionArg)
           .collect(Collectors.toList())
    );
  }

  @Override
  public UnresolvedExpression visitString(StringContext ctx) {
    return AstDSL.stringLiteral(unquoteIdentifier(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitSignedDecimal(SignedDecimalContext ctx) {
    return AstDSL.intLiteral(Integer.valueOf(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitSignedReal(SignedRealContext ctx) {
    return AstDSL.doubleLiteral(Double.valueOf(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitBoolean(BooleanContext ctx) {
    return AstDSL.booleanLiteral(Boolean.valueOf(ctx.getText()));
  }

}
