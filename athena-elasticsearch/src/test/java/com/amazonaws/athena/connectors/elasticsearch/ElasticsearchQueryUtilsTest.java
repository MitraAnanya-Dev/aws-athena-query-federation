/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.Operator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the ElasticsearchQueryUtils class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchQueryUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryUtilsTest.class);

    private final BlockAllocatorImpl allocator = new BlockAllocatorImpl();
    Schema mapping;
    Map<String, ValueSet> constraintsMap = new HashMap<>();

    @Before
    public void setUp()
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .addField("mykeyword", Types.MinorType.VARCHAR.getType())
                .addField(new Field("mylong", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mylong",
                                FieldType.nullable(Types.MinorType.BIGINT.getType()), null))))
                .addField("myinteger", Types.MinorType.INT.getType())
                .addField("myshort", Types.MinorType.INT.getType())
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .addField(new Field("myscaled",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.51")), null))
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .addField("myhalf", Types.MinorType.FLOAT4.getType())
                .addField("mydatemilli", Types.MinorType.DATEMILLI.getType())
                .addField(new Field("mydatenano", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mydatenano",
                                FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null))))
                .addField("myboolean", Types.MinorType.BIT.getType())
                .addField("mybinary", Types.MinorType.VARCHAR.getType())
                .addField("mynested", Types.MinorType.STRUCT.getType(), ImmutableList.of(
                        new Field("l1long", FieldType.nullable(Types.MinorType.BIGINT.getType()), null),
                        new Field("l1date", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null),
                        new Field("l1nested", FieldType.nullable(Types.MinorType.STRUCT.getType()), ImmutableList.of(
                                new Field("l2short", FieldType.nullable(Types.MinorType.LIST.getType()),
                                        Collections.singletonList(new Field("l2short",
                                                FieldType.nullable(Types.MinorType.INT.getType()), null))),
                                new Field("l2binary", FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                                        null))))).build();
    }

    @Test
    public void getProjectionTest()
    {
        logger.info("getProjectionTest - enter");

        List<String> expectedProjection = new ArrayList<>();
        mapping.getFields().forEach(field -> expectedProjection.add(field.getName()));

        // Get the actual projection and compare to the expected one.
        FetchSourceContext context = ElasticsearchQueryUtils.getProjection(mapping);
        List<String> actualProjection = ImmutableList.copyOf(context.includes());

        logger.info("Projections - Expected: {}, Actual: {}", expectedProjection, actualProjection);
        assertEquals("Projections do not match", expectedProjection, actualProjection);

        logger.info("getProjectionTest - exit");
    }

    @Test
    public void getRangePredicateTest()
    {
        logger.info("getRangePredicateTest - enter");

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(),
                                1955, false, 1972, true),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1996),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(_exists_:year) AND year:([* TO 1950} OR {1955 TO 1972] OR [2010 TO *] OR 1952 OR 1996)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getRangePredicateTest - exit");
    }

    @Test
    public void getWhitelistedEquitableValuesPredicate()
    {
        logger.info("getWhitelistedEquitableValuesPredicate - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                true, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
                Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getWhitelistedEquitableValuesPredicate - exit");
    }

    @Test
    public void getExclusiveEquitableValuesPredicate()
    {
        logger.info("getExclusiveEquitableValuesPredicate - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                false, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "NOT age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getExclusiveEquitableValuesPredicate - exit");
    }

    @Test
    public void getAllValuePredicate()
    {
        logger.info("getAllValuePredicate - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), true, true));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(_exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getAllValuePredicate - exit");
    }

    @Test
    public void getNoneValuePredicate()
    {
        logger.info("getNoneValuePredicate - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), false, false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(NOT _exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getNoneValuePredicate - exit");
    }

    @Test
    public void testBuildFilterPredicatesFromPlan_withNullPlan()
    {
        Map<String, List<ColumnPredicate>> result =
                ElasticsearchQueryUtils.buildFilterPredicatesFromPlan(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildFilterPredicatesFromPlan_withNoRelations()
    {
        Plan emptyPlan = Plan.newBuilder().build();
        Map<String, List<ColumnPredicate>> result =
                ElasticsearchQueryUtils.buildFilterPredicatesFromPlan(emptyPlan);
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest()
    @MethodSource("getInputTestMakeQueryFromPlanForDifferentOperator")
    public void testMakeQueryFromPlanForDifferentOperator(Operator op, Object value, String expected)
    {
        Map<String, List<ColumnPredicate>> preds =
                oneColumn("col", mockPredicates(op, value));
        QueryBuilder queryBuilder = ElasticsearchQueryUtils.makeQueryFromPlan(preds);
        assertEquals(expected, queryBuilder.queryName());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getInputTestMakeQueryFromPlanWithMultiPredicates")
    public void testMakeQueryFromPlanWithMultiPredicates(String description,
                                                      Map<String, List<ColumnPredicate>> preds,
                                                      String expectedQuery)
    {
        QueryBuilder qb = ElasticsearchQueryUtils.makeQueryFromPlan(preds);
        assertEquals(expectedQuery, qb.queryName());
    }

    private static Stream<Arguments> getInputTestMakeQueryFromPlanWithMultiPredicates()
    {
        return Stream.of(
                // Case 1: Same column, multiple predicates
                Arguments.of(
                        "same column ANDed in order",
                        new LinkedHashMap<String, List<ColumnPredicate>>() {{
                            put("age", Arrays.asList(
                                    mockPredicates(Operator.GREATER_THAN_OR_EQUAL_TO, 18),
                                    mockPredicates(Operator.LESS_THAN, 65)
                            ));
                        }},
                        "age:[18 TO *] AND age:{* TO 65}"
                ),
                // Case 2: Multiple columns ANDed in insertion order
                Arguments.of(
                        "multiple columns ANDed in insertion order",
                        new LinkedHashMap<String, List<ColumnPredicate>>() {{
                            put("active", Collections.singletonList(mockPredicates(Operator.EQUAL, true)));
                            put("email", Collections.singletonList(mockPredicates(Operator.IS_NOT_NULL, null)));
                        }},
                        "active:true AND (_exists_:email)"
                )
        );
    }

    private static Stream<Arguments> getInputTestMakeQueryFromPlanForDifferentOperator()
    {
        return Stream.of(
                Arguments.of(Operator.EQUAL, 42, "col:42"),
                Arguments.of(Operator.NOT_EQUAL, 100, "NOT col:100"),
                Arguments.of(Operator.GREATER_THAN, 50, "col:{50 TO *}"),
                Arguments.of(Operator.GREATER_THAN_OR_EQUAL_TO, 1970, "col:[1970 TO *]"),
                Arguments.of(Operator.LESS_THAN, 25, "col:{* TO 25}"),
                Arguments.of(Operator.LESS_THAN_OR_EQUAL_TO, 30, "col:[* TO 30]"),
                Arguments.of(Operator.EQUAL, "open", "col:open"),
                Arguments.of(Operator.IS_NULL, null, "(NOT _exists_:col)"),
                Arguments.of(Operator.IS_NOT_NULL, null, "(_exists_:col)")
        );
    }

    private static Map<String, List<ColumnPredicate>> oneColumn(String col, ColumnPredicate... cps)
    {
        Map<String, List<ColumnPredicate>> m = new LinkedHashMap<>();
        m.put(col, Arrays.asList(cps));
        return m;
    }

    private static ColumnPredicate mockPredicates(Operator op, Object value)
    {
        ColumnPredicate p = mock(ColumnPredicate.class);
        when(p.getOperator()).thenReturn(op);
        when(p.getValue()).thenReturn(value);
        return p;
    }

}
