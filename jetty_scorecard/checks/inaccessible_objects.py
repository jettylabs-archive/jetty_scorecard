from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.checks.common import any_object_privileges_by_role
from jetty_scorecard.env import SnowflakeEnvironment, PrivilegeGrant
from jetty_scorecard.util import render_check_template
import pandas as pd


def create() -> Check:
    """Create a check for inaccessible objects

    Looks for objects that are inaccessible because of missing db or schema
    level permissions

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Inaccessible Tables and Views",
        (
            "Check for permissioned tables and views that are inaccessible because of"
            " missing database or schema-level permissions"
        ),
        (
            "For a database object such as a function, table, or view to be accessible,"
            " an active role must have permissions on both the database and the schema."
            " If one of those privileges is missing the object will remain"
            " inaccessible, even if object-level permissions have been"
            " granted.<br><br>This Check looks for users who have been granted"
            " object-level permissions on tables and views, without also being granted"
            " the requisite database- and schema-level permissions.<br><br> Note: This"
            " check does not yet take <em>database role</em> privileges into account."
            " You can read more about this Snowflake preview feature <a"
            ' href="https://docs.snowflake.com/en/user-guide/security-access-control-considerations.html#label-access-control-considerations-database-roles">here</a>.'
            " It also ignores role combinations (using secondary roles) and the admin"
            " capabilities of the <code>ACCOUNTADMIN</code> role (experimentation"
            " appears to indicate that it does not face this same restriction)."
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

    joined_tables = any_object_privileges_by_role(env)

    # Remove ACCOUNTADMIN (they don't seem to have this issue)
    joined_tables = joined_tables[joined_tables.grantee != "ACCOUNTADMIN"]

    missing_db_permissions = joined_tables[~joined_tables.has_db_permission]
    missing_schema_permissions = joined_tables[~joined_tables.has_db_permission]

    if len(missing_db_permissions) > 0 or len(missing_schema_permissions) > 0:
        score = 0.49
    else:
        score = 1.0

    records = joined_tables[
        (~joined_tables.has_db_permission) | (~joined_tables.has_schema_permission)
    ].to_records()

    missing_permissions = {}
    for record in records:
        mut_val = missing_permissions.get(record.grantee, [])
        mut_val += [
            (
                record.object,
                record.has_db_permission,
                record.has_schema_permission,
            )
        ]
        missing_permissions[record.grantee] = mut_val

    if len(missing_permissions) > 0:
        details = render_check_template(
            "inaccessible_objects.html.jinja",
            {
                "missing_permissions": missing_permissions,
            },
        )
    else:
        details = "No unintentionally inaccessible objects detected!"

    return score, details
