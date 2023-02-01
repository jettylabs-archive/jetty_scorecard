from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, FutureGrant
from jetty_scorecard.util import (
    fqn_type,
    FQNType,
    truncated_database,
    render_check_template,
)
from random import random


def create() -> Check:
    """Create a check for applied network policies.

    Network policies are a straightforward and effective way of limiting who
    has access to a Snowflake instance, and in what setting that access is
    allowed.

    Returns:
        Check: The network policy check.
    """
    return Check(
        "Shadow Future Grants",
        "Check for the presence roles with ignored or <em>shadow</em> future grants",
        (
            "Future grants are used to define grants that should automatically be"
            " applied when new schemas, tables, or views are created. These grants can"
            " be set at a database or a schema level. <br><br>If grants are set for an"
            " asset type (e.g., TABLE) at both the database and schema level, for one"
            " or more roles, all database level future grants for that asset type in"
            " that schema will be ignored (for all roles). This can lead to situations"
            " <em>shadow</em> grants where future grants are defined in the system, but"
            " never applied.<br><br>This check looks for roles with shadow"
            " database-level future grants and no schema-level grants."
        ),
        [
            (
                "https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html#considerations",
                "Snowflake Documentation",
            ),
            (
                "https://community.snowflake.com/s/article/Behavior-of-future-grants-when-defined-at-both-database-and-schema-level",
                "Example (Snowflake Community)",
            ),
        ],
        [FutureGrant],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Checks to see if there are any shadow future grants

    Looks at all future grants and organizes them by DB. Checks to see db-level
    grants are overridden (shadow) schema-level grants, leaving users with future
    grants unassigned.

    Returns:
        float: Score
        str: Details
    """
    if not env.has_data or env.future_grants is None:
        return None, "Unable to load future grants."

    # Build a map of future grants at the db and schema level

    future_grant_map = {}

    for x in env.future_grants:
        if fqn_type(x.set_on) == FQNType.DATABASE:
            future_grant_map[(x.set_on, x.asset_type)] = future_grant_map.get(
                (x.set_on, x.asset_type), {"grantees": {}, "schemas": {}}
            )
            future_grant_map[(x.set_on, x.asset_type)]["grantees"][x.grantee] = True
        else:
            future_grant_map[(truncated_database(x.set_on), x.asset_type)] = (
                future_grant_map.get(
                    (truncated_database(x.set_on), x.asset_type),
                    {"grantees": {}, "schemas": {}},
                )
            )
            future_grant_map[(truncated_database(x.set_on), x.asset_type)]["schemas"][
                x.set_on
            ] = future_grant_map[(truncated_database(x.set_on), x.asset_type)][
                "schemas"
            ].get(
                x.set_on, {}
            )
            future_grant_map[(truncated_database(x.set_on), x.asset_type)]["schemas"][
                x.set_on
            ][x.grantee] = True

    # Now for each db, see if there are any schemas that don't have all the necessary grantees
    missing_roles: tuple[str, list[str]] = []
    for db in future_grant_map:
        for schema in future_grant_map[db]["schemas"]:
            overridden_roles: list[str] = []
            for grantee in future_grant_map[db]["grantees"]:
                if grantee not in future_grant_map[db]["schemas"][schema]:
                    overridden_roles.append(grantee)
            if len(overridden_roles) > 0:
                missing_roles.append(((schema, db[1]), overridden_roles))

    num_dbs = len(future_grant_map)
    num_affected_dbs = len(set([truncated_database(x[0][0]) for x in missing_roles]))

    if num_affected_dbs > 0:
        details = render_check_template(
            "shadow_future_grants.html.jinja", {"missing_roles": missing_roles}
        )
    else:
        details = (
            "You don't have any schema-level future grants that override database-level"
            " grants without accounting for all the relevant roles 🎉"
        )

    if not env.has_data:
        score = None
    elif num_dbs == 0:
        score = -1
    else:
        score = 1 - num_affected_dbs / num_dbs

    return score, details
