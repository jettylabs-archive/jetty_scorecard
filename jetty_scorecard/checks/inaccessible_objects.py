from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, PrivilegeGrant
from jetty_scorecard.util import (
    render_string_template,
    truncated_database,
    truncated_schema,
)
import pandas as pd


def create() -> Check:
    """Create a check for inaccessible objects

    Looks for objects that are inaccessible because of missing db or schema
    level permissions

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Inaccessible Objects",
        (
            "Check for permissioned objects that are inaccessible because of missing"
            " database or schema-level permissions"
        ),
        (
            "For a database object such as a function, table, or view to be accessible,"
            " an active role must have permissions on both the database and the schema."
            " If one of those privileges is missing the object will remain"
            " inaccessible, even if object-level permissions have been"
            " granted.<br><br>This Check looks for users who have been granted"
            " object-level permissions, without also being granted the requisite"
            " database- and schema-level permissions.<br><br> Note: This check does not"
            " yet take <em>database role</em> privileges into account. You can read"
            " more about this Snowflake preview feature <a"
            ' href="https://docs.snowflake.com/en/user-guide/security-access-control-considerations.html#label-access-control-considerations-database-roles">here</a>.'
            " It also ignores the <code>ACCOUNTADMIN</code> role as it does not seem to"
            " face this same restriction."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-considerations.html#accessing-database-objects",
                "Accessing Database Objects (Snowflake Documentation)",
            ),
        ],
        [PrivilegeGrant],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for inaccessible objects

    Score is .49 for any inaccessible objects, 1 for none.

    If there is no information, it is None


    Returns:
        float: Score
        str: Details
    """
    if not env.has_data or env.login_history is None:
        return None, "Unable to object permissions."

    dbs = pd.DataFrame(
        [
            {"db": x.asset, "grantee": x.grantee, "has_db_permission": True}
            for x in env.privilege_grants
            if x.asset_type == "DATABASE"
        ]
    ).drop_duplicates()
    schemas = pd.DataFrame(
        [
            {"schema": x.asset, "grantee": x.grantee, "has_schema_permission": True}
            for x in env.privilege_grants
            if x.asset_type == "SCHEMA" and x.privilege in ("OWNERSHIP", "USAGE")
        ]
    ).drop_duplicates()
    tables = pd.DataFrame(
        [
            {
                "object": x.asset,
                "db": truncated_database(x.asset),
                "schema": truncated_schema(x.asset),
                "grantee": x.grantee,
            }
            for x in env.privilege_grants
            if x.asset_type not in ("SCHEMA", "DATABASE")
            and x.privilege in ("OWNERSHIP", "USAGE")
            and x.grantee not in ("", "ACCOUNTADMIN")
        ]
    ).drop_duplicates()

    joined_tables = (
        tables[tables.db != '"SNOWFLAKE"']
        .merge(dbs, how="left")
        .merge(schemas, how="left")
    )

    missing_db_permissions = joined_tables[joined_tables.has_db_permission.isnull()]
    missing_schema_permissions = joined_tables[joined_tables.has_db_permission.isnull()]

    if len(missing_db_permissions) > 0 or len(missing_schema_permissions) > 0:
        score = 0.49
    else:
        score = 1.0

    records = joined_tables[
        joined_tables.has_db_permission.isnull()
        | joined_tables.has_schema_permission.isnull()
    ].to_records()

    missing_permissions = {}
    for record in records:
        mut_val = missing_permissions.get(record.grantee, [])
        mut_val += [
            (
                record.object,
                record.has_db_permission == True,
                record.has_schema_permission == True,
            )
        ]
        missing_permissions[record.grantee] = mut_val

    if len(missing_permissions) > 0:
        details = render_string_template(
            """The following users are unable to access the specified objects because of missing permissions at the schema or database
level:
<ul>
    {% for (role, details) in missing_permissions.items() %}
    <li>{{role}}
        <ul>
            {% for item in details %}
            <li>
                <code>{{ item[0] }}</code> - ({% if (not item[1]) and (not item[2]) -%}
                schema and database level
                {%- elif not(item[1]) %}
                schema level
                {%- else -%}
                database level
                {%- endif %})
            </li>
            {% endfor %}
        </ul>
    </li>
    {% endfor %}
</ul>

You can grant the nessesary privileges by running
<code>GRANT USAGE ON { DATABASE | SCHEMA } &lt;db_or_schema_name&gt; TO ROLE &lt;role_name&gt</code>""",
            {
                "missing_permissions": missing_permissions,
            },
        )
    else:
        details = "No unintentionally inaccessible objects detected!"

    return score, details
