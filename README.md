# 🔍 Snowflake Data Profiling Stored Procedure

This repo contains a Python-based Snowflake stored procedure that automates data profiling for any table. It generates stats like nulls, distinct counts, min/max values, and more — in one shot.

## 💡 Features
- Works for all column types
- Handles nulls, blanks, duplicates, and more
- Returns a consistent summary table
- Accepts database, schema, and table name as input

## 🚀 Usage

```sql
CALL data_profiling_py('MY_DB', 'PUBLIC', 'CUSTOMERS');
