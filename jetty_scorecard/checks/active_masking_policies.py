from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.util import render_check_template
from jetty_scorecard.env import (
    SnowflakeEnvironment,
    MaskingPolicy,
    MaskingPolicyReference,
)


def create() -> Check:
    """Create a check for active masking policies.

    Returns:
        Check: The Check instance.
    """
    return Check(
        "Active Masking Policies",
        "Check for active masking policies",
        (
            "Masking policies make it possible to mask specific data, depending on"
            " criteria such as who is running a query. For example, a masking policy"
            " could be used to mask email addresses for everyone except customer"
            " service representatives."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-column-ddm-intro.html#understanding-dynamic-data-masking",
                "Understanding Dynamic Data Masking (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/user-guide/security-column-ddm-use.html",
                "Using Dynamic Data Masking (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy.html",
                "CREATE MASKING POLICY (Snowflake Documentation)",
            ),
        ],
        [MaskingPolicy, MaskingPolicyReference],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check the environment for masking policies

    If there are no masking policies, return -1 (Info)
    If there are masking policies:
        If we can't see if they're active, return -2 (Insight) with existing policies
        If they are all active and don't have errors, return 1
        If any have errors, return .48
        If they aren't active (but don't have errors), return .75

    Returns:
        float: Score
        str: Details
    """
    # Check for masking policies
    if env.masking_policies is None or len(env.masking_policies) == 0:
        score = -1
        details = (
            "Your account doesn't appear to have any masking policies. Masking policies"
            " are available as part of the Snowflake Enterprise Edition and give data"
            " owners fine-grained control over who can see what data. For example,"
            " masking policies make it easy to hide PII like phone numbers or email"
            " addresses for some, but not all users.<br><br>These policies can also be"
            " combined with tags to mask whole classes of data."
        )

    elif env.masking_policy_references is None:
        policy_names = [x.fqn() for x in env.masking_policies]
        score = -2
        details = render_check_template(
            "active_masking_policies_list.html.jinja",
            {"policy_names": policy_names},
        )

    else:
        policy_set = set([x.fqn() for x in env.masking_policies])
        references_set = set([x.fqn() for x in env.masking_policy_references])

        unused_policies = policy_set.difference(references_set)
        misapplied_policies = [
            (x.fqn(), x.target_fqn, x.status)
            for x in env.masking_policy_references
            if x.status != "ACTIVE"
        ]
        active_policies = [
            (x.fqn(), x.target_fqn)
            for x in env.masking_policy_references
            if x.status == "ACTIVE"
        ]

        if len(misapplied_policies) > 0:
            score = 0.48
        elif len(unused_policies) > 0:
            score = 0.75
        else:
            score = 1

        details = (
            render_check_template(
                "active_masking_policies.html.jinja",
                {
                    "misapplied_policies": misapplied_policies,
                    "unused_policies": unused_policies,
                    "active_policies": active_policies,
                },
            ),
        )

    return score, details
