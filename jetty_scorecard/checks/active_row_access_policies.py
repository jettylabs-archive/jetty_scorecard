from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.util import render_check_template
from jetty_scorecard.env import (
    SnowflakeEnvironment,
    RowAccessPolicy,
    RowAccessPolicyReference,
)


def create() -> Check:
    """Create a check for active row access policies.

    Returns:
        Check: The Check instance.
    """
    return Check(
        "Active Row Access Policies",
        "Check for active row access policies",
        (
            "Row access policies make it possible to return only specific data,"
            " depending on criteria such as who is running a query. For example, a row"
            " access policy could be used to filter sales data to prospects in the"
            " user's sales territory. By using mapping tables, this easily scale to"
            " meet the needs of large teams."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-row-intro.html",
                "Understanding Row Access Policies (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/user-guide/security-row-using.html",
                "Using Using Row Access Policies (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy.html",
                "CREATE ROW ACCESS POLICY (Snowflake Documentation)",
            ),
        ],
        [RowAccessPolicy, RowAccessPolicyReference],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check the environment for row access policies

    If there are no row access policies, return -1 (Info)
    If there are row access policies:
        If we can't see if they're active, return -2 (Insight) with existing policies
        If they are all active and don't have errors, return 1
        If any have errors, return .48
        If they aren't active (but don't have errors), return .75

    Returns:
        float: Score
        str: Details
    """
    # Check for masking policies
    if env.row_access_policies is None or len(env.row_access_policies) == 0:
        score = -1
        details = (
            "Your account doesn't appear to have any row access policies. Row access"
            " policies are available as part of the Snowflake Enterprise Edition and"
            " give data owners fine-grained control over who can see what data."
        )

    elif env.row_access_policy_references is None:
        policy_names = [x.fqn() for x in env.row_access_policies]
        score = -2
        details = render_check_template(
            "active_row_access_policies_list.html.jinja",
            {"policy_names": policy_names},
        )

    else:
        policy_set = set([x.fqn() for x in env.row_access_policies])
        references_set = set([x.fqn() for x in env.row_access_policy_references])

        unused_policies = policy_set.difference(references_set)
        misapplied_policies = [
            (x.fqn(), x.target_fqn, x.status)
            for x in env.row_access_policy_references
            if x.status != "ACTIVE"
        ]
        active_policies = [
            (x.fqn(), x.target_fqn)
            for x in env.row_access_policy_references
            if x.status == "ACTIVE"
        ]

        if len(misapplied_policies) > 0:
            score = 0.48
        elif len(unused_policies) > 0:
            score = 0.75
        else:
            score = 1

        return (
            1,
            render_check_template(
                "active_row_access_policies.html.jinja",
                {
                    "misapplied_policies": misapplied_policies,
                    "unused_policies": unused_policies,
                    "active_policies": active_policies,
                },
            ),
        )
