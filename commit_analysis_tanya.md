# AWS Athena Query Federation - Commit Analysis Document

## Executive Summary

**Commit ID:** `dafd145c94fa4dc566ec3bc7ecc305193a428b97`  
**Repository:** aws-athena-query-federation (tanyaseh fork)  
**Date:** August 18, 2025  
**Author:** Tanya Sehgal  
**Message:** "Add jdbc substrait changes"

### Overview
This commit introduces Substrait query plan integration for JDBC connectors with enhanced FAS token support 
and improved credential management. The changes focus on enabling Substrait-based query processing across the federation framework.

### Key Metrics
- **Files Modified:** 14
- **Lines Added:** 580
- **Lines Deleted:** 34
- **Net Change:** +546 lines
- **Packages Affected:** 4

---

## Change Categories

### 🚀 Major Features
- **Substrait Query Plan Integration** - Complete JDBC support for Substrait-based queries
- **Enhanced FAS Token Support** - Improved credential management across handlers
- **SQL Visitor Pattern Implementation** - Advanced query AST processing

### 🔧 Infrastructure Improvements
- **Credential Provider Refactoring** - Streamlined AWS client creation
- **Dependency Version Management** - Updated Substrait and Calcite versions
- **Package Structure Enhancement** - New SDK tools package

---

## Detailed Change Analysis

### Package: athena-federation-sdk-tools

#### New Substrait Utilities

**File:** `SubstraitSqlUtils.java` *(NEW)*
```
Changes: +66 -0 lines
Impact: High - New core functionality
```

**Method-Level Analysis:**

1. **`deserializeSubstraitPlan(String planString, SqlDialect sqlDialect)` - NEW (STATIC)**
   - **Purpose:** Converts Base64-encoded Substrait plan to Calcite SqlNode
   - **Parameters:** 
     - `planString`: Base64-encoded Substrait plan
     - `sqlDialect`: Target SQL dialect for conversion
   - **Returns:** SqlNode representing the SQL AST
   - **Process Flow:**
     ```java
     1. Create ProtoPlanConverter instance
     2. Initialize SubstraitToCalcite with SimpleExtension defaults
     3. Decode Base64 plan string to bytes
     4. Parse bytes to Substrait Plan proto
     5. Convert proto to Substrait plan object
     6. Extract root input and convert to RelNode
     7. Use RelToSqlConverter to generate SqlNode
     ```
   - **Exception Handling:** Wraps IOException in RuntimeException
   - **Impact:** Core functionality for Substrait integration

### Package: athena-federation-sdk

#### Core Infrastructure Changes

**File:** `FederationRequestHandler.java`
```
Changes: +25 -18 lines
Impact: High - Core infrastructure
```

**Method-Level Changes:**

1. **`getS3Client(AwsRequestOverrideConfiguration, S3Client)` - REFACTORED**
   - **Enhancement:** Simplified using extracted `getAwsCredentialsProvider()` method
   - **Impact:** Reduced code duplication, improved maintainability

2. **`getAthenaClient(AwsRequestOverrideConfiguration, AthenaClient)` - REFACTORED**
   - **Enhancement:** Uses common credential provider extraction
   - **Impact:** Consistent credential handling

3. **`getSecretsManagerClient(AwsRequestOverrideConfiguration, SecretsManagerClient)` - NEW**
   - **Purpose:** Creates SecretsManagerClient with credential override support
   - **Impact:** Enables FAS token support for secrets management

4. **`getAwsCredentialsProvider(AwsRequestOverrideConfiguration)` - NEW (PRIVATE)**
   - **Purpose:** Extracts credential provider from override configuration
   - **Impact:** Eliminates code duplication across client creation methods

**File:** `MetadataHandler.java`
```
Changes: +16 -4 lines
Impact: High - Security enhancement
```

**Method-Level Changes:**

1. **Field Changes:**
   - **Before:** `private final CachableSecretsManager secretsManager;`
   - **After:** `private CachableSecretsManager secretsManager;`
   - **Impact:** Allows dynamic initialization based on FAS token

2. **`getSecretsManager()` - NEW (PROTECTED)**
   - **Purpose:** Provides access to the CachableSecretsManager instance
   - **Impact:** Enables subclass access to secrets manager

3. **`doHandleRequest(BlockAllocator, MetadataRequest, OutputStream)` - ENHANCED**
   - **Enhancement:** Added FAS token detection and dynamic secrets manager initialization
   - **Impact:** Context-aware credential management

**File:** `RecordHandler.java`
```
Changes: +13 -1 lines
Impact: Medium - Security enhancement
```

**Method-Level Changes:**

1. **Field Changes:**
   - **Before:** `private final CachableSecretsManager secretsManager;`
   - **After:** `private CachableSecretsManager secretsManager;`
   - **Impact:** Consistent with MetadataHandler pattern

2. **`getSecretsManager()` - NEW (PROTECTED)**
   - **Purpose:** Provides access to secrets manager
   - **Impact:** Enables subclass customization

3. **`doHandleRequest(BlockAllocator, RecordRequest, OutputStream)` - ENHANCED**
   - **Enhancement:** Added FAS token support for READ_RECORDS operations
   - **Impact:** Consistent credential handling across operations

### Package: athena-jdbc

#### Dependency Management

**File:** `pom.xml`
```
Changes: +6 -0 lines
Impact: Medium - Build configuration
```

**Configuration Analysis:**

1. **New Dependency Addition:**
   ```xml
   <dependency>
       <groupId>com.amazonaws</groupId>
       <artifactId>athena-federation-sdk-tools</artifactId>
       <version>2022.47.1</version>
       <scope>compile</scope>
   </dependency>
   ```
   - **Impact:** Enables access to Substrait utilities in JDBC package

#### Query Processing Enhancement

**File:** `JdbcSplitQueryBuilder.java`
```
Changes: +163 -0 lines
Impact: Very High - Core functionality
```

**Method-Level Analysis:**

1. **`prepareStatementWithSql(Connection, Constraints, String, String, String, String)` - ENHANCED**
   - **Enhancement:** Added Substrait query plan detection
   - **New Logic:** Routes Substrait queries to specialized processing
   - **Impact:** Enables Substrait query execution

2. **`getSqlDialect()` - NEW (PROTECTED)**
   - **Purpose:** Returns SQL dialect for database
   - **Default:** AnsiSqlDialect.DEFAULT
   - **Impact:** Enables database-specific SQL generation

3. **`appendLimitOffsetWithValue(String limit, String offset)` - NEW (PROTECTED)**
   - **Purpose:** Formats LIMIT clause with specific values
   - **Default:** `"LIMIT " + limit`
   - **Impact:** Supports database-specific LIMIT/OFFSET syntax

4. **`prepareStatementWithSql(Connection, Constraints, SqlDialect, Split, String, String, String, String)` - NEW (PROTECTED)**
   - **Purpose:** Core Substrait query processing method
   - **Process Flow:**
     ```java
     1. Extract Base64-encoded Substrait plan
     2. Deserialize plan using SubstraitSqlUtils
     3. Process WHERE clause with visitor patterns
     4. Handle ORDER BY and LIMIT/OFFSET
     5. Bind parameters to PreparedStatement
     ```
   - **Parameter Binding:** Supports BIGINT, INTEGER, VARCHAR, DECIMAL, DATE types
   - **Impact:** Complete Substrait query execution pipeline

#### New Supporting Classes

**File:** `BaseSchemaAwareConverter.java` *(NEW)*
```
Changes: +66 -0 lines
Impact: Medium - Supporting utility
```

**Method-Level Analysis:**

1. **`makeCalciteTableFromBaseSchema(NamedStruct schema)` - NEW (STATIC)**
   - **Purpose:** Converts Substrait NamedStruct to Calcite AbstractTable
   - **Impact:** Schema conversion between Substrait and Calcite

2. **`substraitTypeToCalcite(RelDataTypeFactory factory, Type t)` - NEW (PRIVATE STATIC)**
   - **Purpose:** Maps Substrait types to Calcite RelDataType
   - **Supported Types:** I32 → INTEGER, I64 → BIGINT, VARCHAR → VARCHAR
   - **Impact:** Type system bridge

**File:** `SubstraitTypeAndValue.java` *(NEW)*
```
Changes: +54 -0 lines
Impact: Low - Data structure
```

**Method-Level Analysis:**

1. **Constructor:** Validates type and value parameters
2. **Accessors:** `getType()` and `getValue()` methods
3. **`toString()`:** Debugging support

**File:** `FilterRemovalVisitor.java` *(NEW)*
```
Changes: +85 -0 lines
Impact: Medium - Query optimization
```

**Method-Level Analysis:**

1. **`visit(SqlCall call)` - OVERRIDDEN**
   - **Purpose:** Removes partition-based filters from SQL AST
   - **Logic:** Replaces target column conditions with boolean literals
   - **Impact:** Query optimization for partition handling

**File:** `SubstraitAccumulatorVisitor.java` *(NEW)*
```
Changes: +54 -0 lines
Impact: Medium - Query processing
```

**Method-Level Analysis:**

1. **`visit(SqlLiteral literal)` - OVERRIDDEN**
   - **Purpose:** Converts literals to parameters for prepared statements
   - **Logic:** Extracts literal values and replaces with SqlDynamicParam
   - **Impact:** Enables parameterized query execution

### Package: athena-snowflake

#### Database-Specific Enhancements

**File:** `SnowflakeQueryStringBuilder.java`
```
Changes: +18 -1 lines
Impact: Medium - Database support
```

**Method-Level Analysis:**

1. **`appendLimitOffset(Split split)` - ENHANCED**
   - **Enhancement:** Uses new `appendLimitOffsetWithValue()` method
   - **Impact:** Consistent with parent class pattern

2. **`getSqlDialect()` - NEW (OVERRIDDEN)**
   - **Purpose:** Returns SnowflakeSqlDialect.DEFAULT
   - **Impact:** Enables Snowflake-specific SQL generation

3. **`appendLimitOffsetWithValue(String limit, String offset)` - NEW (OVERRIDDEN)**
   - **Purpose:** Snowflake-specific LIMIT/OFFSET syntax
   - **Syntax:** Uses lowercase "limit" and "offset" keywords
   - **Impact:** Database compliance

**File:** `SnowflakeCredentialsProvider.java`
```
Changes: +6 -1 lines
Impact: Low - Code quality
```

**Method-Level Analysis:**

1. **Constructor Enhancement:** Added overloaded constructor for direct CachableSecretsManager injection
2. **Impact:** Better dependency injection support

**File:** `SnowflakeMetadataHandler.java` & `SnowflakeRecordHandler.java`
```
Changes: +1 -1 lines each
Impact: Low - Integration improvement
```

**Method-Level Analysis:**

1. **`getCredentialProvider()` - ENHANCED**
   - **Enhancement:** Uses shared secrets manager from parent class
   - **Impact:** Consistent resource management

### Package: Root

#### Dependency Version Management

**File:** `pom.xml`
```
Changes: +6 -6 lines
Impact: Medium - Version management
```

**Version Changes:**
- **AWS SDK v2:** 2.31.77 → 2.31.54 (downgrade)
- **Substrait Isthmus:** 0.52.0 → 0.48.0 (downgrade)
- **Substrait Core:** 0.52.0 → 0.48.0 (downgrade)
- **Calcite Server:** 1.39.0 → 1.37.0 (downgrade)
- **Calcite Linq4j:** 1.39.0 → 1.37.0 (downgrade)
- **Calcite Core:** 1.39.0 → 1.37.0 (downgrade)

**Impact:** Version alignment for compatibility

---

## Technical Impact Assessment

### Method Signature Changes Summary

#### New Public/Protected Methods
```java
// FederationRequestHandler.java
default SecretsManagerClient getSecretsManagerClient(AwsRequestOverrideConfiguration, SecretsManagerClient)

// MetadataHandler.java & RecordHandler.java
protected CachableSecretsManager getSecretsManager()

// JdbcSplitQueryBuilder.java
protected SqlDialect getSqlDialect()
protected String appendLimitOffsetWithValue(String limit, String offset)

// SnowflakeQueryStringBuilder.java
@Override protected SqlDialect getSqlDialect()
@Override protected String appendLimitOffsetWithValue(String limit, String offset)
```

#### New Static Utility Methods
```java
// SubstraitSqlUtils.java
public static SqlNode deserializeSubstraitPlan(String planString, SqlDialect sqlDialect)

// BaseSchemaAwareConverter.java
public static AbstractTable makeCalciteTableFromBaseSchema(NamedStruct schema)
```

#### Field Visibility Changes
```java
// MetadataHandler.java & RecordHandler.java
- private final CachableSecretsManager secretsManager;
+ private CachableSecretsManager secretsManager;
```

### High Impact Changes
1. **Substrait Integration** - Complete JDBC support for advanced query processing
2. **FAS Token Enhancement** - Dynamic credential management
3. **Visitor Pattern Implementation** - Advanced SQL AST processing

### Medium Impact Changes
1. **Dependency Version Alignment** - Compatibility improvements
2. **Database Dialect Support** - Enhanced database-specific features
3. **Package Structure** - New SDK tools package

### Low Impact Changes
1. **Code Refactoring** - Improved maintainability
2. **Constructor Overloading** - Better dependency injection

---

## Risk Assessment

### Low Risk
- Version downgrades (compatibility focused)
- Code refactoring with preserved functionality
- New utility classes with isolated functionality

### Medium Risk
- Field visibility changes (requires testing)
- Enhanced credential management
- Database dialect changes

### High Risk
- Substrait integration (complex new functionality)
- Core query processing changes
- Visitor pattern implementations

---

## Testing Recommendations

### Unit Testing
- [ ] Test all new Substrait utility classes
- [ ] Validate visitor pattern implementations
- [ ] Test credential provider changes

### Integration Testing
- [ ] End-to-end Substrait query execution
- [ ] FAS token integration
- [ ] Database-specific dialect functionality

### Performance Testing
- [ ] Query execution performance with Substrait
- [ ] Memory usage with visitor patterns
- [ ] Credential management efficiency

---

## Deployment Considerations

### Prerequisites
- Verify Substrait and Calcite dependency compatibility
- Ensure FAS token configuration
- Test with downgraded dependency versions

### Rollback Plan
- Previous commit available for rollback
- No database schema changes
- Configuration backward compatible

### Monitoring Points
- Query execution times
- Error rates in new code paths
- Memory usage patterns

---

## Conclusion

This commit provides comprehensive Substrait integration for JDBC connectors with enhanced security features. The version downgrades suggest compatibility-focused approach, while the new functionality significantly expands query processing capabilities.

**Recommendation:** Proceed with thorough testing of Substrait integration and dependency compatibility validation.
