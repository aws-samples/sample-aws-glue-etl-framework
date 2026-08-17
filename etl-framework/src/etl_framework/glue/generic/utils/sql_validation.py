"""
SQL Validation Utilities.

Provides functions to validate and sanitize user-provided values that are
interpolated into SQL statements. Prevents SQL injection attacks by ensuring
identifiers, filters, and queries conform to safe patterns.

Security principle: Never trust configuration values in SQL contexts.
All values from JSON configs (DynamoDB) must be validated before use in
Spark SQL, JDBC queries, or DDL/DML statements.
"""

import re
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

# Pattern for valid SQL identifiers:
# Must start with a letter or underscore, followed by letters, digits, or underscores.
# Allows dots for qualified names (catalog.database.table) when validated separately.
_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Pattern for qualified identifiers (e.g., "catalog.database.table")
_QUALIFIED_IDENTIFIER_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
)

# SQL keywords that indicate destructive operations (case-insensitive)
_DANGEROUS_DDL_KEYWORDS = [
    "DROP",
    "CREATE",
    "ALTER",
    "GRANT",
    "REVOKE",
    "EXEC",
    "EXECUTE",
    "CALL",
    "XP_",
    "SP_",
]

_DANGEROUS_DML_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "TRUNCATE",
    "REPLACE",
]

# Combined list for read-only validation
_DANGEROUS_KEYWORDS_ALL = _DANGEROUS_DDL_KEYWORDS + _DANGEROUS_DML_KEYWORDS

# Keywords dangerous in filters (prevent subqueries and unions)
_DANGEROUS_FILTER_KEYWORDS = [
    "DROP",
    "CREATE",
    "ALTER",
    "GRANT",
    "REVOKE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "EXEC",
    "EXECUTE",
    "UNION",
    "INTO",
    "CALL",
    "SELECT",
]

# Allowed pre/post action keywords for Redshift
_ALLOWED_PREPOST_KEYWORDS = [
    "TRUNCATE",
    "DELETE",
    "ANALYZE",
    "VACUUM",
    "BEGIN",
    "COMMIT",
    "END",
]


class SQLValidationError(ValueError):
    """Raised when SQL validation fails due to potentially unsafe input."""

    pass


def validate_identifier(value: str, field_name: str = "identifier") -> str:
    """
    Validate that a value is a safe SQL identifier.

    A safe identifier contains only letters, digits, and underscores,
    and starts with a letter or underscore.

    Args:
        value: The identifier value to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated identifier string

    Raises:
        SQLValidationError: If the identifier contains unsafe characters
    """
    if not value:
        raise SQLValidationError(
            f"Invalid {field_name}: identifier cannot be empty"
        )

    if not _IDENTIFIER_PATTERN.match(value):
        raise SQLValidationError(
            f"Invalid {field_name}: '{value}'. "
            f"Identifiers must contain only letters, digits, and underscores, "
            f"and must start with a letter or underscore."
        )

    return value


def validate_qualified_identifier(
    value: str, field_name: str = "identifier"
) -> str:
    """
    Validate a dot-qualified SQL identifier (e.g., "catalog.database.table").

    Each component must be a valid simple identifier.

    Args:
        value: The qualified identifier to validate (e.g., "mydb.mytable")
        field_name: Name of the field (for error messages)

    Returns:
        The validated qualified identifier string

    Raises:
        SQLValidationError: If any component contains unsafe characters
    """
    if not value:
        raise SQLValidationError(
            f"Invalid {field_name}: qualified identifier cannot be empty"
        )

    if not _QUALIFIED_IDENTIFIER_PATTERN.match(value):
        raise SQLValidationError(
            f"Invalid {field_name}: '{value}'. "
            f"Qualified identifiers must consist of dot-separated components, "
            f"each containing only letters, digits, and underscores."
        )

    return value


def validate_column_name(value: str, field_name: str = "column") -> str:
    """
    Validate that a value is a safe column name.

    Same rules as identifier, but with a more specific error message.

    Args:
        value: The column name to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated column name

    Raises:
        SQLValidationError: If the column name is unsafe
    """
    if not value:
        raise SQLValidationError(
            f"Invalid {field_name}: column name cannot be empty"
        )

    if not _IDENTIFIER_PATTERN.match(value):
        raise SQLValidationError(
            f"Invalid {field_name}: '{value}'. "
            f"Column names must contain only letters, digits, and underscores, "
            f"and must start with a letter or underscore."
        )

    return value


def validate_read_only_sql(sql: str, field_name: str = "source_sql") -> str:
    """
    Validate that a SQL string is a read-only SELECT statement.

    Rejects SQL containing DDL (DROP, CREATE, ALTER) or DML
    (INSERT, UPDATE, DELETE, TRUNCATE) keywords.

    Args:
        sql: The SQL query to validate
        field_name: Name of the field (for error messages)

    Returns:
        The validated SQL string

    Raises:
        SQLValidationError: If the SQL contains dangerous operations
    """
    if not sql or not sql.strip():
        raise SQLValidationError(
            f"Invalid {field_name}: SQL query cannot be empty"
        )

    normalized = sql.strip()

    # Must start with SELECT (or WITH for CTEs)
    upper_sql = normalized.upper().lstrip()
    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH")):
        raise SQLValidationError(
            f"Invalid {field_name}: SQL must be a SELECT statement "
            f"(optionally starting with WITH for CTEs). "
            f"Got: '{normalized[:50]}...'"
        )

    # Check for dangerous keywords as standalone words
    for keyword in _DANGEROUS_KEYWORDS_ALL:
        # Use word boundary to avoid false positives (e.g., "UPDATED_AT" matching "UPDATE")
        pattern = rf"\b{keyword}\b"
        if re.search(pattern, normalized, re.IGNORECASE):
            raise SQLValidationError(
                f"Invalid {field_name}: SQL contains disallowed keyword '{keyword}'. "
                f"Only read-only SELECT statements are permitted."
            )

    # Check for multiple statements (semicolons followed by non-whitespace)
    if re.search(r";\s*\S", normalized):
        raise SQLValidationError(
            f"Invalid {field_name}: Multiple SQL statements are not allowed. "
            f"Only single SELECT statements are permitted."
        )

    return sql


def validate_source_filter(
    filter_expr: str, field_name: str = "source_filter"
) -> str:
    """
    Validate a Spark DataFrame filter expression.

    Allows simple SQL predicates but rejects DDL/DML keywords,
    UNION operations, and subqueries that could access other views.

    Args:
        filter_expr: The filter expression string
        field_name: Name of the field (for error messages)

    Returns:
        The validated filter expression

    Raises:
        SQLValidationError: If the filter contains dangerous patterns
    """
    if not filter_expr or not filter_expr.strip():
        raise SQLValidationError(
            f"Invalid {field_name}: filter expression cannot be empty"
        )

    normalized = filter_expr.strip()

    # Check for dangerous keywords
    for keyword in _DANGEROUS_FILTER_KEYWORDS:
        pattern = rf"\b{keyword}\b"
        if re.search(pattern, normalized, re.IGNORECASE):
            raise SQLValidationError(
                f"Invalid {field_name}: filter contains disallowed keyword '{keyword}'. "
                f"Filters should be simple predicates "
                f"(e.g., \"status = 'active' AND date > '2024-01-01'\")."
            )

    # Check for semicolons (statement separators)
    if ";" in normalized:
        raise SQLValidationError(
            f"Invalid {field_name}: filter cannot contain semicolons. "
            f"Only single predicate expressions are allowed."
        )

    # Check for comments (could hide injected SQL)
    if "--" in normalized or "/*" in normalized:
        raise SQLValidationError(
            f"Invalid {field_name}: filter cannot contain SQL comments (-- or /*)."
        )

    return filter_expr


def validate_pre_post_actions(
    sql: str, field_name: str = "pre_actions"
) -> str:
    """
    Validate Redshift pre/post-action SQL statements.

    Only allows a restricted set of operations: TRUNCATE, DELETE,
    ANALYZE, VACUUM, BEGIN, COMMIT, END.

    Blocks dangerous operations like DROP, CREATE, ALTER, GRANT,
    INSERT INTO (external tables), EXEC, and multi-statement attacks.

    Args:
        sql: The pre/post-action SQL string
        field_name: Name of the field (for error messages)

    Returns:
        The validated SQL string

    Raises:
        SQLValidationError: If the SQL contains disallowed operations
    """
    if not sql or not sql.strip():
        # Empty is valid (no pre/post actions)
        return sql

    normalized = sql.strip()

    # Split on semicolons to validate each statement
    statements = [s.strip() for s in normalized.split(";") if s.strip()]

    for statement in statements:
        upper_stmt = statement.upper().lstrip()

        # Check each statement starts with an allowed keyword
        statement_valid = False
        for allowed in _ALLOWED_PREPOST_KEYWORDS:
            if upper_stmt.startswith(allowed):
                statement_valid = True
                break

        if not statement_valid:
            raise SQLValidationError(
                f"Invalid {field_name}: statement '{statement[:80]}...' "
                f"starts with a disallowed operation. "
                f"Only the following are allowed: {', '.join(_ALLOWED_PREPOST_KEYWORDS)}."
            )

        # Even within allowed statements, block dangerous sub-patterns
        for keyword in _DANGEROUS_DDL_KEYWORDS:
            if keyword in _ALLOWED_PREPOST_KEYWORDS:
                continue
            pattern = rf"\b{keyword}\b"
            if re.search(pattern, statement, re.IGNORECASE):
                raise SQLValidationError(
                    f"Invalid {field_name}: statement contains disallowed keyword "
                    f"'{keyword}' within a {field_name} action."
                )

        # Block comments within statements
        if "--" in statement or "/*" in statement:
            raise SQLValidationError(
                f"Invalid {field_name}: statements cannot contain SQL comments."
            )

    return sql


def validate_temp_view_name(source_key: str) -> str:
    """
    Validate and construct a safe temp view name from a source key.

    The temp view name is constructed as "temp_{source_key}", ensuring
    the source_key portion contains only safe identifier characters.

    Args:
        source_key: The source key from configuration

    Returns:
        The validated temp view name (e.g., "temp_my_source")

    Raises:
        SQLValidationError: If the source_key contains unsafe characters
    """
    if not source_key:
        raise SQLValidationError(
            "Invalid source_key: cannot be empty when creating temp view"
        )

    # Validate the source_key as a safe identifier
    if not _IDENTIFIER_PATTERN.match(source_key):
        raise SQLValidationError(
            f"Invalid source_key: '{source_key}'. "
            f"Source keys used as temp view names must contain only letters, "
            f"digits, and underscores, and start with a letter or underscore."
        )

    return f"temp_{source_key}"


def validate_column_list(
    columns: List[str], field_name: str = "columns"
) -> List[str]:
    """
    Validate a list of column names.

    Args:
        columns: List of column name strings
        field_name: Name of the field (for error messages)

    Returns:
        The validated list of column names

    Raises:
        SQLValidationError: If any column name is invalid
    """
    if not columns:
        return columns

    validated = []
    for col in columns:
        # Allow column references with dots (table.column) and stars
        if col == "*":
            validated.append(col)
        elif "." in col:
            validate_qualified_identifier(col, f"{field_name} item")
            validated.append(col)
        else:
            validate_column_name(col, f"{field_name} item")
            validated.append(col)

    return validated


def backtick_escape_identifier(value: str) -> str:
    """
    Escape an identifier using backticks for use in Spark SQL.

    This provides defense-in-depth: even after validation, identifiers
    are wrapped in backticks to prevent any edge-case injection.

    Args:
        value: A pre-validated identifier

    Returns:
        The backtick-escaped identifier (e.g., "`my_table`")
    """
    # Remove any existing backticks to prevent double-escaping
    cleaned = value.replace("`", "")
    return f"`{cleaned}`"


def build_safe_qualified_name(
    *parts: str, escape: bool = True
) -> str:
    """
    Build a safe qualified SQL name from validated parts.

    Each part is validated as an identifier and optionally backtick-escaped.

    Args:
        *parts: Identifier parts (e.g., catalog, database, table)
        escape: Whether to backtick-escape each part (default: True)

    Returns:
        A safe qualified name like "`catalog`.`database`.`table`"

    Raises:
        SQLValidationError: If any part fails validation
    """
    validated_parts = []
    for part in parts:
        if not part:
            continue
        validate_identifier(part, "qualified name component")
        if escape:
            validated_parts.append(backtick_escape_identifier(part))
        else:
            validated_parts.append(part)

    if not validated_parts:
        raise SQLValidationError("Cannot build qualified name: no valid parts provided")

    return ".".join(validated_parts)
