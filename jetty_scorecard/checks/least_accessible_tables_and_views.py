from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.checks.common import user_object_access
from jetty_scorecard.env import SnowflakeEnvironment, PrivilegeGrant, RoleGrant
from jetty_scorecard.util import (
    render_check_template,
    extract_schema,
    truncated_database,
)
import pandas as pd
import numpy as np


def create() -> Check:
    """Create a check for most accessible objects

    Looks for objects that are accessible to the highest number of users

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Least-Accessible Tables and Views",
        "Find the tables and views that are accessible by the fewest users",
        (
            "This check looks for the tables and views that are accessible to the"
            " fewest users. It ignores database roles (currently a Snowflake Preview"
            " feature), <code>SNOWFLAKE</code> and <code>SNOWFLAKE_SAMPLE_DATA</code>"
            " databases, <code>INFORMATION_SCHEMA</code> schemas and any admin-specific"
            " permissions that the ACCOUNTADMIN role may have.<br><br>Limiting access"
            " users-level access to match the specific user needs aligns with the"
            " principle of least privilege (PoLP) and helps create a more secure"
            " environment."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-overview.html",
                "Overview of Access Control (Snowflake Documentation)",
            ),
            (
                "https://en.wikipedia.org/wiki/Principle_of_least_privilege",
                "Principle of least privilege (Wikipedia)",
            ),
        ],
        [PrivilegeGrant, RoleGrant],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for least accessible tables and views

    Score is -2 (Insight)

    If there is no information, it is None


    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return None, "Unable to read object permissions."

    access = user_object_access(env)
    # Get all tables/views (only tables/views are included in env.entities)
    # just in case any have no access granted
    all_objects = pd.DataFrame([{"object": x.fqn()} for x in env.entities])

    object_access = all_objects.merge(access, how="left")
    # Filter out the dbs/schemas we want to ignore
    object_access = object_access[
        (
            ~object_access["object"]
            .apply(truncated_database)
            .isin(['"SNOWFLAKE"', '"SNOWFLAKE_SAMPLE_DATA"'])
        )
        & (
            ~object_access["object"]
            .apply(extract_schema)
            .isin(['"INFORMATION_SCHEMA"'])
        )
    ]

    # For all objects, the set of users with access
    set_list = object_access.groupby("object")["user"].apply(set)

    # Get 10 least accessible tables/views
    least_accessible = (
        set_list.where(set_list != set({np.nan}), None)
        .str.len()
        .fillna(0)
        .astype(int)
        .sort_index()
        .sort_values(ascending=True, kind="mergesort")
        .head(10)
        .to_frame()
        .to_records()
    )

    return -2, render_check_template(
        "least_accessible_objects.html.jinja",
        {"least_accessible": least_accessible},
    )
