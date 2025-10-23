/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class OracleQueryStringBuilderTest
{
    private OracleQueryStringBuilder queryBuilder;
    
    @Mock
    private Split mockSplit;
    
    @Mock
    private Constraints mockConstraints;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        queryBuilder = new OracleQueryStringBuilder("\"", new OracleFederationExpressionParser("\""));
    }

    @Test
    public void testGetFromClauseWithSplitNoPartitions()
    {
        // Test FROM clause generation without partitions
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(OracleMetadataHandler.ALL_PARTITIONS);
        
        String result = queryBuilder.getFromClauseWithSplit("testCatalog", "testSchema", "testTable", mockSplit);
        
        assertEquals(" FROM \"testCatalog\".\"testSchema\".\"testTable\" ", result);
    }

    @Test
    public void testGetFromClauseWithSplitWithPartition()
    {
        // Test FROM clause generation with specific partition
        String partitionName = "P_2023_01";
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(partitionName);
        when(mockSplit.getProperties())
                .thenReturn(ImmutableMap.of(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, partitionName));
        
        String result = queryBuilder.getFromClauseWithSplit("testCatalog", "testSchema", "testTable", mockSplit);
        
        assertTrue("Should contain partition clause", result.contains("PARTITION"));
        assertTrue("Should contain partition name", result.contains(partitionName));
        assertEquals(" FROM \"testCatalog\".\"testSchema\".\"testTable\" PARTITION (" + partitionName + ") ", result);
    }

    @Test
    public void testGetFromClauseWithSplitNoCatalog()
    {
        // Test FROM clause generation without catalog
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(OracleMetadataHandler.ALL_PARTITIONS);
        
        String result = queryBuilder.getFromClauseWithSplit(null, "testSchema", "testTable", mockSplit);
        
        assertEquals(" FROM \"testSchema\".\"testTable\" ", result);
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        // Test that partition where clauses return empty list (Oracle uses partition syntax in FROM clause)
        when(mockSplit.getProperties()).thenReturn(ImmutableMap.of("test", "value"));
        
        assertEquals("Should return empty list", Collections.emptyList(), queryBuilder.getPartitionWhereClauses(mockSplit));
    }

    @Test
    public void testAppendLimitOffsetBasic()
    {
        // Test Oracle-specific FETCH FIRST syntax instead of LIMIT
        when(mockConstraints.getLimit()).thenReturn(100L);
        
        String result = queryBuilder.appendLimitOffset(mockSplit, mockConstraints);
        
        assertEquals(" FETCH FIRST 100 ROWS ONLY ", result);
    }

    @Test
    public void testFromClauseQuoting()
    {
        // Test that identifiers are properly quoted with Oracle quote character
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(OracleMetadataHandler.ALL_PARTITIONS);
        
        String result = queryBuilder.getFromClauseWithSplit("test-catalog", "test-schema", "test-table", mockSplit);
        
        assertTrue("Should quote catalog", result.contains("\"test-catalog\""));
        assertTrue("Should quote schema", result.contains("\"test-schema\""));
        assertTrue("Should quote table", result.contains("\"test-table\""));
    }

    @Test
    public void testOracleSpecificPartitionSyntax()
    {
        // Test Oracle-specific partition syntax in FROM clause
        String partitionName = "P_SALES_2023_Q1";
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(partitionName);
        when(mockSplit.getProperties())
                .thenReturn(ImmutableMap.of(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, partitionName));
        
        String result = queryBuilder.getFromClauseWithSplit("PROD", "SALES", "ORDERS", mockSplit);
        
        // Verify Oracle partition syntax: TABLE_NAME PARTITION (partition_name)
        String expected = " FROM \"PROD\".\"SALES\".\"ORDERS\" PARTITION (" + partitionName + ") ";
        assertEquals("Should use Oracle partition syntax", expected, result);
    }

    @Test
    public void testLimitClauseForSubstraitQueries()
    {
        // Test FETCH FIRST syntax for Substrait-generated queries
        when(mockConstraints.getLimit()).thenReturn(5000L);
        
        String result = queryBuilder.appendLimitOffset(mockSplit, mockConstraints);
        
        assertEquals("Should use Oracle FETCH FIRST syntax", " FETCH FIRST 5000 ROWS ONLY ", result);
        assertTrue("Should not contain LIMIT keyword", !result.contains("LIMIT"));
    }

    @Test
    public void testLimitClauseWithLargeLimit()
    {
        // Test Oracle FETCH FIRST with large limit values
        when(mockConstraints.getLimit()).thenReturn(1000000L);
        
        String result = queryBuilder.appendLimitOffset(mockSplit, mockConstraints);
        
        assertEquals("Should handle large limits with FETCH FIRST", " FETCH FIRST 1000000 ROWS ONLY ", result);
        assertTrue("Should not contain LIMIT keyword", !result.contains("LIMIT"));
    }

    @Test
    public void testFromClauseWithUnderscorePartition()
    {
        // Test Oracle partition syntax with underscore partition names
        String partitionName = "SALES_2024_Q1";
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(partitionName);
        when(mockSplit.getProperties())
                .thenReturn(ImmutableMap.of(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, partitionName));
        
        String result = queryBuilder.getFromClauseWithSplit("PROD", "SALES", "ORDERS", mockSplit);
        
        String expected = " FROM \"PROD\".\"SALES\".\"ORDERS\" PARTITION (" + partitionName + ") ";
        assertEquals("Should use Oracle partition syntax with underscores", expected, result);
    }

    @Test
    public void testFromClauseWithComplexPartitionNames()
    {
        // Test FROM clause with complex partition names
        String partitionName = "SALES_Q1_2024_REGION_WEST";
        when(mockSplit.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))
                .thenReturn(partitionName);
        when(mockSplit.getProperties())
                .thenReturn(ImmutableMap.of(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, partitionName));
        
        String result = queryBuilder.getFromClauseWithSplit("PROD", "SALES", "ORDERS", mockSplit);
        
        String expected = " FROM \"PROD\".\"SALES\".\"ORDERS\" PARTITION (" + partitionName + ") ";
        assertEquals("Should handle complex partition names", expected, result);
    }

}
