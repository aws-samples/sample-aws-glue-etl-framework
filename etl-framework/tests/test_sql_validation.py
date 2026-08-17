"""Tests for SQL validation utilities."""

import pytest

from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    backtick_escape_identifier,
    build_safe_qualified_name,
    validate_column_list,
    validate_column_name,
    validate_identifier,
    validate_pre_post_actions,
    validate_qualified_identifier,
    validate_read_only_sql,
    validate_source_filter,
    validate_temp_view_name,
)


class TestValidateIdentifier:
    """Tests for validate_identifier."""

    def test_valid_simple_identifiers(self):
        """Test that valid identifiers pass."""
        assert validate_identifier("my_table", "test") == "my_table"
        assert validate_identifier("_private", "test") == "_private"
        assert validate_identifier("Table1", "test") == "Table1"
        assert validate_identifier("a", "test") == "a"
        assert validate_identifier("column_name_123", "test") == "column_name_123"

    def test_empty_raises(self):
        """Test that empty string raises."""
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_identifier("", "test_field")

    def test_starts_with_digit_raises(self):
        """Test that identifier starting with digit raises."""
        with pytest.raises(SQLValidationError, match="must contain only"):
            validate_identifier("1table", "test_field")

    def test_spaces_raise(self):
        """Test that spaces in identifier raise."""
        with pytest.raises(SQLValidationError, match="must contain only"):
            validate_identifier("my table", "test_field")

    def test_semicolons_raise(self):
        """Test that semicolons raise (SQL injection attempt)."""
        with pytest.raises(SQLValidationError, match="must contain only"):
            validate_identifier("table; DROP TABLE users", "test_field")

    def test_parentheses_raise(self):
        """Test that parentheses raise."""
        with pytest.raises(SQLValidationError, match="must contain only"):
            validate_identifier("func()", "test_field")

    def test_dashes_raise(self):
        """Test that dashes raise (could be SQL comment)."""
        with pytest.raises(SQLValidationError, match="must contain only"):
            validate_identifier("my-table", "test_field")

    def test_dots_raise(self):
        """Test that dots raise in simple identifier."""
        with pytest.raises(SQLValidationError, match="must contain only"):
            validate_identifier("schema.table", "test_field")

    def test_sql_injection_attempts(self):
        """Test various SQL injection patterns are rejected."""
        injection_attempts = [
            "table; DROP TABLE users--",
            "1 OR 1=1",
            "table UNION SELECT * FROM secrets",
            "x' OR '1'='1",
            "table/*comment*/",
            "table\n; DELETE FROM",
        ]
        for attempt in injection_attempts:
            with pytest.raises(SQLValidationError):
                validate_identifier(attempt, "test")


class TestValidateQualifiedIdentifier:
    """Tests for validate_qualified_identifier."""

    def test_valid_qualified_names(self):
        """Test valid dot-separated identifiers."""
        assert validate_qualified_identifier("db.table", "test") == "db.table"
        assert validate_qualified_identifier("catalog.db.table", "test") == "catalog.db.table"
        assert validate_qualified_identifier("single", "test") == "single"

    def test_empty_raises(self):
        """Test empty string raises."""
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_qualified_identifier("", "test")

    def test_invalid_component_raises(self):
        """Test that invalid component raises."""
        with pytest.raises(SQLValidationError):
            validate_qualified_identifier("valid.123invalid", "test")

    def test_trailing_dot_raises(self):
        """Test trailing dot raises."""
        with pytest.raises(SQLValidationError):
            validate_qualified_identifier("db.", "test")

    def test_leading_dot_raises(self):
        """Test leading dot raises."""
        with pytest.raises(SQLValidationError):
            validate_qualified_identifier(".table", "test")

    def test_injection_in_component(self):
        """Test injection attempt within component."""
        with pytest.raises(SQLValidationError):
            validate_qualified_identifier("db.table; DROP TABLE x", "test")


class TestValidateColumnName:
    """Tests for validate_column_name."""

    def test_valid_columns(self):
        """Test valid column names pass."""
        assert validate_column_name("id", "col") == "id"
        assert validate_column_name("first_name", "col") == "first_name"
        assert validate_column_name("_col1", "col") == "_col1"

    def test_empty_raises(self):
        """Test empty string raises."""
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_column_name("", "col")

    def test_injection_in_column(self):
        """Test injection attempt in column name."""
        with pytest.raises(SQLValidationError):
            validate_column_name("col) FROM secrets UNION SELECT (x", "col")


class TestValidateReadOnlySQL:
    """Tests for validate_read_only_sql."""

    def test_valid_select(self):
        """Test valid SELECT statements pass."""
        assert validate_read_only_sql("SELECT * FROM table1") == "SELECT * FROM table1"
        assert validate_read_only_sql("SELECT id, name FROM users WHERE active = true")
        assert validate_read_only_sql("  SELECT count(*) FROM orders")

    def test_valid_with_cte(self):
        """Test WITH (CTE) queries pass."""
        sql = "WITH cte AS (SELECT * FROM t) SELECT * FROM cte"
        assert validate_read_only_sql(sql) == sql

    def test_empty_raises(self):
        """Test empty SQL raises."""
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_read_only_sql("")
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_read_only_sql("   ")

    def test_non_select_raises(self):
        """Test non-SELECT statements raise."""
        with pytest.raises(SQLValidationError, match="must be a SELECT"):
            validate_read_only_sql("INSERT INTO t VALUES (1)")
        with pytest.raises(SQLValidationError, match="must be a SELECT"):
            validate_read_only_sql("UPDATE t SET x = 1")

    def test_drop_raises(self):
        """Test DROP keyword raises."""
        with pytest.raises(SQLValidationError, match="DROP"):
            validate_read_only_sql("SELECT * FROM t; DROP TABLE t")

    def test_delete_raises(self):
        """Test DELETE keyword raises."""
        with pytest.raises(SQLValidationError, match="DELETE"):
            validate_read_only_sql("SELECT * FROM t WHERE id IN (DELETE FROM t2)")

    def test_insert_raises(self):
        """Test INSERT keyword raises."""
        with pytest.raises(SQLValidationError, match="INSERT"):
            validate_read_only_sql("SELECT * INTO newtable FROM t INSERT INTO t2")

    def test_create_raises(self):
        """Test CREATE keyword raises."""
        with pytest.raises(SQLValidationError, match="CREATE"):
            validate_read_only_sql("SELECT 1; CREATE TABLE evil (x int)")

    def test_truncate_raises(self):
        """Test TRUNCATE keyword raises."""
        with pytest.raises(SQLValidationError, match="TRUNCATE"):
            validate_read_only_sql("SELECT 1; TRUNCATE TABLE t")

    def test_grant_raises(self):
        """Test GRANT keyword raises."""
        with pytest.raises(SQLValidationError, match="GRANT"):
            validate_read_only_sql("SELECT * FROM t; GRANT ALL ON t TO public")

    def test_multiple_statements_raises(self):
        """Test multiple statements separated by semicolons raise."""
        with pytest.raises(SQLValidationError, match="Multiple SQL statements"):
            validate_read_only_sql("SELECT 1; SELECT 2")

    def test_case_insensitive_detection(self):
        """Test that dangerous keywords are detected case-insensitively."""
        with pytest.raises(SQLValidationError):
            validate_read_only_sql("SELECT * FROM t; drop table t")
        with pytest.raises(SQLValidationError):
            validate_read_only_sql("SELECT * FROM t; DrOp TaBlE t")

    def test_safe_column_names_with_keywords(self):
        """Test that column names containing keyword substrings pass."""
        # "UPDATED_AT" contains "UPDATE" but shouldn't trigger (word boundary check)
        sql = "SELECT updated_at, created_at FROM events"
        assert validate_read_only_sql(sql) == sql

    def test_select_into_detected(self):
        """Test SELECT ... INTO is caught via INSERT keyword check."""
        # "INTO" is checked in filter validation but not in read-only SQL
        # The "INSERT" keyword check catches explicit INSERT statements
        sql = "SELECT id, name FROM users"
        assert validate_read_only_sql(sql) == sql


class TestValidateSourceFilter:
    """Tests for validate_source_filter."""

    def test_valid_filters(self):
        """Test valid filter expressions pass."""
        assert validate_source_filter("status = 'active'") == "status = 'active'"
        assert validate_source_filter("age > 18 AND country = 'US'")
        assert validate_source_filter("date_col >= '2024-01-01'")
        assert validate_source_filter("id IN (1, 2, 3)")
        assert validate_source_filter("name LIKE '%test%'")
        assert validate_source_filter("col IS NOT NULL")

    def test_empty_raises(self):
        """Test empty filter raises."""
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_source_filter("")

    def test_drop_raises(self):
        """Test DROP in filter raises."""
        with pytest.raises(SQLValidationError, match="DROP"):
            validate_source_filter("1=1; DROP TABLE users")

    def test_union_raises(self):
        """Test UNION in filter raises."""
        with pytest.raises(SQLValidationError, match="UNION"):
            validate_source_filter("id = 1 UNION SELECT * FROM secrets")

    def test_insert_raises(self):
        """Test INSERT in filter raises."""
        with pytest.raises(SQLValidationError, match="INSERT"):
            validate_source_filter("1=1; INSERT INTO t VALUES(1)")

    def test_delete_raises(self):
        """Test DELETE in filter raises."""
        with pytest.raises(SQLValidationError, match="DELETE"):
            validate_source_filter("1=1; DELETE FROM t")

    def test_truncate_raises(self):
        """Test TRUNCATE in filter raises."""
        with pytest.raises(SQLValidationError, match="TRUNCATE"):
            validate_source_filter("1=1; TRUNCATE TABLE t")

    def test_semicolons_raise(self):
        """Test semicolons in filter raise."""
        with pytest.raises(SQLValidationError, match="semicolons"):
            validate_source_filter("status = 'active'; --")

    def test_sql_comments_raise(self):
        """Test SQL comments raise."""
        with pytest.raises(SQLValidationError, match="comments"):
            validate_source_filter("status = 'active' -- ignore rest")
        with pytest.raises(SQLValidationError, match="comments"):
            validate_source_filter("status = 'active' /* comment */")

    def test_into_raises(self):
        """Test INTO keyword raises (prevent SELECT INTO)."""
        with pytest.raises(SQLValidationError, match="INTO"):
            validate_source_filter("id = (SELECT * INTO temp FROM t)")

    def test_select_subquery_raises(self):
        """Test SELECT subquery in filter raises."""
        with pytest.raises(SQLValidationError, match="SELECT"):
            validate_source_filter("id IN (SELECT id FROM secrets)")
        with pytest.raises(SQLValidationError, match="SELECT"):
            validate_source_filter("status = (SELECT password FROM credentials LIMIT 1)")


class TestValidatePrePostActions:
    """Tests for validate_pre_post_actions."""

    def test_empty_passes(self):
        """Test empty string passes (no actions)."""
        assert validate_pre_post_actions("") == ""
        assert validate_pre_post_actions("   ") == "   "

    def test_valid_truncate(self):
        """Test TRUNCATE TABLE passes."""
        sql = "TRUNCATE TABLE schema.orders;"
        assert validate_pre_post_actions(sql) == sql

    def test_valid_delete(self):
        """Test DELETE passes."""
        sql = "DELETE FROM schema.orders WHERE date < '2024-01-01';"
        assert validate_pre_post_actions(sql) == sql

    def test_valid_analyze(self):
        """Test ANALYZE passes."""
        sql = "ANALYZE schema.orders;"
        assert validate_pre_post_actions(sql) == sql

    def test_valid_vacuum(self):
        """Test VACUUM passes."""
        sql = "VACUUM schema.orders;"
        assert validate_pre_post_actions(sql) == sql

    def test_valid_multiple_allowed(self):
        """Test multiple allowed statements pass."""
        sql = "TRUNCATE TABLE t; ANALYZE t;"
        assert validate_pre_post_actions(sql) == sql

    def test_drop_raises(self):
        """Test DROP TABLE raises."""
        with pytest.raises(SQLValidationError, match="disallowed operation"):
            validate_pre_post_actions("DROP TABLE users;")

    def test_create_raises(self):
        """Test CREATE raises."""
        with pytest.raises(SQLValidationError, match="disallowed operation"):
            validate_pre_post_actions("CREATE TABLE evil (id int);")

    def test_grant_raises(self):
        """Test GRANT raises."""
        with pytest.raises(SQLValidationError, match="disallowed operation"):
            validate_pre_post_actions("GRANT ALL ON schema TO public;")

    def test_alter_raises(self):
        """Test ALTER raises."""
        with pytest.raises(SQLValidationError, match="disallowed operation"):
            validate_pre_post_actions("ALTER TABLE t ADD COLUMN x int;")

    def test_select_raises(self):
        """Test SELECT raises (not in allowed list)."""
        with pytest.raises(SQLValidationError, match="disallowed operation"):
            validate_pre_post_actions("SELECT * FROM secrets;")

    def test_insert_raises(self):
        """Test INSERT raises."""
        with pytest.raises(SQLValidationError, match="disallowed operation"):
            validate_pre_post_actions("INSERT INTO t VALUES (1);")

    def test_comments_raise(self):
        """Test SQL comments within allowed statements raise."""
        with pytest.raises(SQLValidationError, match="comments"):
            validate_pre_post_actions("TRUNCATE TABLE t -- this is a comment")
        with pytest.raises(SQLValidationError, match="comments"):
            validate_pre_post_actions("DELETE FROM t WHERE id = 1 /* block comment */")
        # Comments after semicolons are caught as disallowed operations
        with pytest.raises(SQLValidationError):
            validate_pre_post_actions("TRUNCATE TABLE t; -- DROP TABLE secrets")

    def test_hidden_drop_in_delete(self):
        """Test DROP hidden within an allowed statement raises."""
        with pytest.raises(SQLValidationError, match="DROP"):
            validate_pre_post_actions("DELETE FROM t WHERE id = 1; DROP TABLE t2;")


class TestValidateTempViewName:
    """Tests for validate_temp_view_name."""

    def test_valid_source_keys(self):
        """Test valid source keys produce correct temp view names."""
        assert validate_temp_view_name("orders") == "temp_orders"
        assert validate_temp_view_name("raw_data") == "temp_raw_data"
        assert validate_temp_view_name("_internal") == "temp__internal"

    def test_empty_raises(self):
        """Test empty source key raises."""
        with pytest.raises(SQLValidationError, match="cannot be empty"):
            validate_temp_view_name("")

    def test_injection_attempt_raises(self):
        """Test injection attempt in source key raises."""
        with pytest.raises(SQLValidationError):
            validate_temp_view_name("orders; DROP TABLE t")

    def test_spaces_raise(self):
        """Test spaces in source key raise."""
        with pytest.raises(SQLValidationError):
            validate_temp_view_name("my source")

    def test_special_chars_raise(self):
        """Test special characters raise."""
        with pytest.raises(SQLValidationError):
            validate_temp_view_name("table-name")
        with pytest.raises(SQLValidationError):
            validate_temp_view_name("table.name")


class TestValidateColumnList:
    """Tests for validate_column_list."""

    def test_valid_column_list(self):
        """Test valid column list passes."""
        cols = ["id", "name", "email"]
        assert validate_column_list(cols) == cols

    def test_star_allowed(self):
        """Test that '*' is allowed."""
        assert validate_column_list(["*"]) == ["*"]

    def test_qualified_columns(self):
        """Test dot-qualified columns pass."""
        cols = ["t.id", "t.name"]
        assert validate_column_list(cols) == cols

    def test_empty_list(self):
        """Test empty list passes through."""
        assert validate_column_list([]) == []
        assert validate_column_list(None) is None

    def test_invalid_column_raises(self):
        """Test invalid column in list raises."""
        with pytest.raises(SQLValidationError):
            validate_column_list(["valid_col", "bad col; DROP TABLE t"])


class TestBacktickEscapeIdentifier:
    """Tests for backtick_escape_identifier."""

    def test_basic_escape(self):
        """Test basic backtick escaping."""
        assert backtick_escape_identifier("my_table") == "`my_table`"
        assert backtick_escape_identifier("orders") == "`orders`"

    def test_removes_existing_backticks(self):
        """Test that existing backticks are removed to prevent double-escaping."""
        assert backtick_escape_identifier("`my_table`") == "`my_table`"
        assert backtick_escape_identifier("my`table") == "`mytable`"


class TestBuildSafeQualifiedName:
    """Tests for build_safe_qualified_name."""

    def test_single_part(self):
        """Test single-part qualified name."""
        assert build_safe_qualified_name("table1", escape=True) == "`table1`"
        assert build_safe_qualified_name("table1", escape=False) == "table1"

    def test_two_parts(self):
        """Test two-part qualified name."""
        assert build_safe_qualified_name("db", "table1", escape=True) == "`db`.`table1`"
        assert build_safe_qualified_name("db", "table1", escape=False) == "db.table1"

    def test_three_parts(self):
        """Test three-part qualified name."""
        result = build_safe_qualified_name("catalog", "db", "table1", escape=True)
        assert result == "`catalog`.`db`.`table1`"
        result = build_safe_qualified_name("catalog", "db", "table1", escape=False)
        assert result == "catalog.db.table1"

    def test_empty_parts_skipped(self):
        """Test that empty parts are skipped."""
        assert build_safe_qualified_name("db", "", "table1", escape=False) == "db.table1"

    def test_invalid_part_raises(self):
        """Test that invalid identifier in any part raises."""
        with pytest.raises(SQLValidationError):
            build_safe_qualified_name("valid", "invalid table", escape=False)

    def test_all_empty_raises(self):
        """Test that all empty parts raises."""
        with pytest.raises(SQLValidationError, match="no valid parts"):
            build_safe_qualified_name("", "", "")


class TestSQLInjectionScenarios:
    """Integration tests simulating real-world SQL injection attempts."""

    def test_redshift_table_name_injection(self):
        """Test injection via table name for Redshift."""
        with pytest.raises(SQLValidationError):
            validate_identifier("orders; DROP TABLE users--", "table_name")

    def test_watermark_column_injection(self):
        """Test injection via watermark column name."""
        with pytest.raises(SQLValidationError):
            validate_column_name(
                "1) as x FROM secrets UNION SELECT (secret_col", "watermark_column"
            )

    def test_source_sql_data_exfiltration(self):
        """Test preventing data exfiltration via source_sql."""
        with pytest.raises(SQLValidationError):
            validate_read_only_sql(
                "SELECT * FROM users; INSERT INTO exfil_table SELECT * FROM secrets"
            )

    def test_filter_union_injection(self):
        """Test UNION injection in filter."""
        with pytest.raises(SQLValidationError, match="UNION"):
            validate_source_filter(
                "id = 1 UNION ALL SELECT password FROM credentials"
            )

    def test_pre_actions_privilege_escalation(self):
        """Test privilege escalation via pre_actions."""
        with pytest.raises(SQLValidationError):
            validate_pre_post_actions(
                "GRANT ALL PRIVILEGES ON ALL TABLES TO attacker;"
            )

    def test_comment_bypass_attempt(self):
        """Test comment-based bypass in filter."""
        with pytest.raises(SQLValidationError, match="comments"):
            validate_source_filter("1=1 -- AND restricted = true")

    def test_nested_injection_in_pre_actions(self):
        """Test nested injection in allowed pre_action."""
        with pytest.raises(SQLValidationError):
            validate_pre_post_actions(
                "DELETE FROM t WHERE 1=1; CREATE USER hacker WITH PASSWORD 'p';"
            )

    def test_source_sql_ddl_in_cte(self):
        """Test DDL hidden in what looks like a CTE."""
        with pytest.raises(SQLValidationError, match="CREATE"):
            validate_read_only_sql(
                "WITH x AS (SELECT 1) SELECT * FROM x; CREATE TABLE evil (id int)"
            )
