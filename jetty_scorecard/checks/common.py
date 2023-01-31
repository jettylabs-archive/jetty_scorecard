"""Functionality common between multiple checks"""

import pandas as pd
from jetty_scorecard.env import SnowflakeEnvironment, RoleGrantNodeType
from jetty_scorecard.util import (
    truncated_database,
    truncated_schema,
)
import networkx as nx


def any_object_privileges_by_role(env: SnowflakeEnvironment) -> pd.DataFrame:
    """Returns a dataframe of object privileges by role

    This excludes schemas and databases

    Args:
        env (SnowflakeEnvironment): environment object

    Returns:
        pd.DataFrame: DataFrame of object privileges by role, including whether
        the role has permissions on the schema and db.

    """
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
            and x.grantee not in ("", "ACCOUNTADMIN")
        ]
    ).drop_duplicates()

    joined_tables = (
        tables[tables.db != '"SNOWFLAKE"']
        .merge(dbs, how="left")
        .merge(schemas, how="left")
    )

    joined_tables["has_db_permission"].fillna(False, inplace=True)
    joined_tables["has_schema_permission"].fillna(False, inplace=True)

    return joined_tables


def user_object_access(env: SnowflakeEnvironment) -> pd.DataFrame:
    """Returns a dataframe of user access to objects

    This excludes schemas and databases, as well as any invalid access (because
    of missing db or schema privileges).

    This also excludes database roles (via an inner join)

    Also excludes disabled users, as they don't have access to anything

    Args:
        env (SnowflakeEnvironment): environment object

    Returns:
        pd.DataFrame: DataFrame of objects and users that can access them

    """
    DG = nx.DiGraph()
    DG.add_edges_from(
        [
            (
                (
                    r.grantee,
                    RoleGrantNodeType.USER
                    if r.grantee_type == "USER"
                    else RoleGrantNodeType.ROLE,
                ),
                (r.role, RoleGrantNodeType.ROLE),
            )
            for r in env.role_grants
        ]
    )

    user_group_map = []
    for user in env.users:
        if (user.name, RoleGrantNodeType.USER) in DG and not user.disabled:
            user_group_map += [
                {"user": user.name, "role": x[0]}
                for x in nx.descendants(DG, (user.name, RoleGrantNodeType.USER))
                if x[1] == RoleGrantNodeType.ROLE
            ]

    role_privileges = any_object_privileges_by_role(env)

    return role_privileges[
        role_privileges.has_db_permission & role_privileges.has_schema_permission
    ].merge(pd.DataFrame(user_group_map), left_on="grantee", right_on="role")
