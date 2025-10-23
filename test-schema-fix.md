# Schema Fix Test Summary

## Problem Analysis
The IndexOutOfBoundsException occurs because:
1. The Substrait plan contains projection operations that reference more columns than are registered in the Calcite schema
2. Our CustomSubstraitToCalcite was registering tables with only a single VARCHAR column
3. When Calcite tries to process projections, it fails because it can't find the expected columns

## Root Cause
The error occurs in `RelBuilder.inferAlias()` when trying to access index 1 in a SingletonImmutablePairList, which means:
- The Substrait plan expects at least 2 columns to be projected
- Our registered table schema only has 1 column
- Calcite fails when trying to infer aliases for the missing columns

## Solution Approach
1. **Enhanced CustomSubstraitToCalcite**: Now accepts a Schema parameter and properly maps Arrow types to SQL types
2. **Modified SubstraitSqlUtils**: Updated to pass the actual table schema to CustomSubstraitToCalcite
3. **Updated JdbcSplitQueryBuilder**: Modified to pass the actual table schema to the Substrait converter

## Key Changes Made

### 1. Enhanced CustomSubstraitToCalcite.java
- Added Schema parameter to constructor
- Modified `getRowType()` to iterate through actual table fields
- **NEW**: Added `mapArrowTypeToSqlType()` method to properly convert Arrow types to Calcite SQL types
- **NEW**: Proper type mapping for INTEGER, DOUBLE, BOOLEAN, DATE, TIMESTAMP, DECIMAL, and VARCHAR

### 2. SubstraitSqlUtils.java
- Added overloaded `deserializeSubstraitPlan()` method accepting tableSchema parameter
- Updated CustomSubstraitToCalcite instantiation to pass schema with proper types

### 3. JdbcSplitQueryBuilder.java
- Updated method signature to accept tableSchema parameter
- Modified method call chain to pass schema through to Substrait converter

## Expected Result
With these changes, the CustomSubstraitToCalcite will:
1. Register tables with the correct number of columns matching the actual database table schema
2. Use proper SQL data types instead of defaulting everything to VARCHAR
3. Prevent the IndexOutOfBoundsException during projection operations
4. Allow Calcite to properly infer aliases for projected columns

## Technical Details
The enhanced type mapping handles:
- Arrow Int → SQL INTEGER
- Arrow FloatingPoint → SQL DOUBLE  
- Arrow Bool → SQL BOOLEAN
- Arrow Date → SQL DATE
- Arrow Timestamp → SQL TIMESTAMP
- Arrow Decimal → SQL DECIMAL
- All other types → SQL VARCHAR (safe fallback)

## Next Steps
1. Build and deploy the updated connector
2. Test with the same query that was failing
3. Verify that the projection operations now work correctly with the proper schema registration
4. Monitor for any additional type-related issues that may surface

## Build Command
```bash
cd /local/home/bahndutr/workplace/aws-athena-query-federation
docker run --rm -v $(pwd):/workspace -w /workspace maven:3.8-openjdk-17 mvn clean package -DskipTests -Dcheckstyle.skip=true -pl athena-oracle -am
```
