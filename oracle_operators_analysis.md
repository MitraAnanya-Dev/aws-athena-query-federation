# Oracle Athena Connector - Supported vs Unsupported Operators Analysis

## Currently Supported by Oracle Athena Connector

Based on `OracleMetadataHandler.java`, the Oracle connector supports **ALL StandardFunctions EXCEPT**:

### **Explicitly Unsupported Functions:**
1. `NULLIF` - `$nullif`
2. `IS_DISTINCT_FROM` - `$is_distinct_from` 
3. `MODULUS` - `$modulus`

### **Currently Supported Functions:**
1. `AND` - `$and`
2. `OR` - `$or` 
3. `NOT` - `$not`
4. `IS_NULL` - `$is_null`
5. `EQUAL` - `$equal`
6. `NOT_EQUAL` - `$not_equal`
7. `LESS_THAN` - `$less_than`
8. `LESS_THAN_OR_EQUAL` - `$less_than_or_equal`
9. `GREATER_THAN` - `$greater_than`
10. `GREATER_THAN_OR_EQUAL` - `$greater_than_or_equal`
11. `ADD` - `$add`
12. `SUBTRACT` - `$subtract`
13. `MULTIPLY` - `$multiply`
14. `DIVIDE` - `$divide`
15. `NEGATE` - `$negate`
16. `LIKE_PATTERN` - `$like_pattern`
17. `IN_PREDICATE` - `$in`
18. `ARRAY_CONSTRUCTOR` - `$array`

## Oracle Database Native Support vs Athena Connector Gap

### **Functions Oracle DB Supports but Athena Connector Does NOT:**

#### **Missing Arithmetic Functions:**
- `MOD()` - Oracle's native modulus function (connector blocks `$modulus`)
- `POWER()` / `**` - Exponentiation
- `SQRT()` - Square root
- `ABS()` - Absolute value
- `CEIL()` / `CEILING()` - Ceiling function
- `FLOOR()` - Floor function
- `ROUND()` - Rounding function
- `TRUNC()` - Truncation function
- `SIGN()` - Sign function

#### **Missing String Functions:**
- `UPPER()` - Convert to uppercase
- `LOWER()` - Convert to lowercase
- `INITCAP()` - Initialize capitals
- `LENGTH()` - String length
- `SUBSTR()` - Substring extraction
- `INSTR()` - Find substring position
- `REPLACE()` - String replacement
- `TRANSLATE()` - Character translation
- `TRIM()` / `LTRIM()` / `RTRIM()` - Trimming functions
- `CONCAT()` / `||` - String concatenation
- `REGEXP_LIKE()` - Regular expression matching
- `REGEXP_REPLACE()` - Regular expression replacement
- `REGEXP_SUBSTR()` - Regular expression substring

#### **Missing Date/Time Functions:**
- `SYSDATE` - Current date/time
- `CURRENT_DATE` - Current date
- `CURRENT_TIMESTAMP` - Current timestamp
- `ADD_MONTHS()` - Add months to date
- `MONTHS_BETWEEN()` - Months between dates
- `NEXT_DAY()` - Next occurrence of day
- `LAST_DAY()` - Last day of month
- `EXTRACT()` - Extract date components
- `TO_DATE()` - Convert to date
- `TO_CHAR()` - Convert to character
- `TRUNC()` (date version) - Truncate date

#### **Missing Aggregate Functions:**
- `COUNT()` - Count rows
- `SUM()` - Sum values
- `AVG()` - Average values
- `MIN()` - Minimum value
- `MAX()` - Maximum value
- `STDDEV()` - Standard deviation
- `VARIANCE()` - Variance
- `LISTAGG()` - List aggregation

#### **Missing Analytical Functions:**
- `ROW_NUMBER()` - Row numbering
- `RANK()` - Ranking
- `DENSE_RANK()` - Dense ranking
- `LAG()` / `LEAD()` - Access previous/next rows
- `FIRST_VALUE()` / `LAST_VALUE()` - First/last values in window

#### **Missing Conditional Functions:**
- `CASE` - Case expressions
- `DECODE()` - Oracle's decode function
- `COALESCE()` - Return first non-null
- `NVL()` - Oracle's null value function
- `NVL2()` - Oracle's enhanced null value function
- `NULLIF()` - Return null if equal (explicitly blocked)

#### **Missing Set Operations:**
- `EXISTS` - Existence check
- `ANY` / `SOME` - Any comparison
- `ALL` - All comparison

#### **Missing Type Conversion:**
- `CAST()` - Type casting
- `TO_NUMBER()` - Convert to number
- `TO_CHAR()` - Convert to character
- `TO_DATE()` - Convert to date

## Recommendation

The Oracle Athena connector should add support for:

### **High Priority (commonly used):**
1. `NULLIF()` - Currently blocked but Oracle supports it
2. `MOD()` / `MODULUS` - Currently blocked but Oracle supports it
3. `UPPER()`, `LOWER()`, `LENGTH()` - Basic string functions
4. `SUBSTR()`, `CONCAT()` - String manipulation
5. `CASE` expressions - Conditional logic
6. `COALESCE()`, `NVL()` - Null handling
7. Basic aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)

### **Medium Priority:**
1. Date/time functions (`EXTRACT`, `ADD_MONTHS`, `TRUNC`)
2. Mathematical functions (`ABS`, `ROUND`, `CEIL`, `FLOOR`)
3. `CAST()` operations
4. `EXISTS` and set operations

### **Low Priority:**
1. Advanced analytical functions
2. Regular expression functions
3. Complex string manipulation functions
