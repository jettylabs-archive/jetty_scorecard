from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.checks.common import user_object_access
from jetty_scorecard.env import SnowflakeEnvironment, PrivilegeGrant, RoleGrant
from jetty_scorecard.util import render_check_template


def create() -> Check:
    """Create a check for most accessible objects

    Looks for objects that are accessible to the highest number of users

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Most-Accessible Objects",
        "Find the objects that are accessible by the most users",
        (
            "This check looks for the database objects (excluding databases and"
            " schemas) that are accessible to the highest number of users. It ignores"
            " database roles (currently a Snowflake Preview feature),"
            " <code>SNOWFLAKE</code> and <code>SNOWFLAKE_SAMPLE_DATA</code> databases,"
            " <code>INFORMATION_SCHEMA</code> schemas and any admin-specific"
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
    """Check for most accessible objects

    Score is -2 (Insight)

    If there is no information, it is None


    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return None, "Unable to read object permissions."

    access = user_object_access(env)
    # each object with a set of users that have access to it
    set_list = access.groupby("object")["user"].apply(set)
    most_accessible = (
        set_list.str.len()
        .sort_index()
        .sort_values(ascending=False, kind="mergesort")
        .head(10)
        .to_frame()
        .to_records()
    )

    return -2, render_check_template(
        "most_accessible_objects.html.jinja",
        {"most_accessible": most_accessible},
    )
