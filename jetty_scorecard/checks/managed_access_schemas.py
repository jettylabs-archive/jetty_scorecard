from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, Schema
from jetty_scorecard.util import render_check_template


def create() -> Check:
    """Create a check for managed access schemas.

    Managed access schemas help centralize privilege management by limiting who
    set access privileges in a schema to the schema owner and roles with the
    MANAGE GRANTS privilege.

    This check looks for managed access schemas, and teaches the user about them
    if they aren't being used.

    Returns:
        Check: The network policy check.
    """
    return Check(
        "Managed Access Schemas",
        "Check for the presence of one or more managed access schemas.",
        (
            "Managed access schemas help centralize privilege management by limiting"
            " who can set access privileges on objects within a schema to the schema"
            " owner and roles with the MANAGE GRANTS privilege. By contrast, in a"
            " non-managed schema, the owner of an object can always set permissions for"
            " that specific object.<br><br>Managed access schemas are easy to"
            " configure, and can greatly simplify privilege management."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-considerations.html#centralizing-grant-management-using-managed-access-schemas",
                (
                    "Centralizing Grant Management Using Managed Access Schemas"
                    " (Snowflake Documentation)"
                ),
            ),
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-configure.html#creating-managed-access-schemas",
                "Creating Managed Access Schemas (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/sql-reference/sql/alter-schema.html",
                "ALTER SCHEMA (Snowflake Documentation)",
            ),
        ],
        [Schema],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check the environment for managed access schemas

    If there is one or more managed access schemas, tell which they are, score = 1
    If there are none, score = -1 (INFO).

    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return (None, "Unable to look for managed access schemas.")

    managed_access_schemas = [x.fqn() for x in env.schemas if x.managed_access]

    if len(managed_access_schemas) > 0:
        return (
            1,
            render_check_template(
                "managed_access_schemas.html.jinja",
                {"managed_access_schemas": managed_access_schemas},
            ),
        )
    else:
        return (
            -1,
            (
                "There are no managed access schemas in your Snowflake account. You can"
                " convert existing schemas to be managed access schemas with the"
                " <code>ALTER SCHEMA &lt;name&gt; ENABLE MANAGED ACCESS</code>. The"
                " <code>WITH MANAGED ACCESS</code> parameter can be use to enable"
                " managed access when creating new schemas."
            ),
        )
