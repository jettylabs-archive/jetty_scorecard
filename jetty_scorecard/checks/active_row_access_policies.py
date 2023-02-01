from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.util import render_string_template
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
            " access policy could be used to filter sales data to accounts relevant"
            " data for a specific employee. By using mapping tables, this easily scale"
            " to meet the needs of large teams."
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
        details = render_string_template(
            """You have the following row access policies in your environment:
<ul>
    {% for policy in policy_names|sort %}
    <li>{{ policy }}</li>
    {% endfor %}
</ul>

You can run Jetty Scorecard again using a role that can access <code>SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES</code>
to check if all of these polices have been successfully applied in at least one location. You can also check this
manually by running <code>"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES WHERE policy_kind = 'ROW_ACCESS_POLICY'</code>.""",
            policy_names=policy_names,
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
            render_string_template(
                """{% if misapplied_policies|length > 0 %}
The following policies are applied to the given objects, but have a status other than <code>ACTIVE</code>:
<ul>
    {% for (policy, target, status) in misapplied_policies|sort(attribute="1,0") %}
    <li>
        <code>{{ policy }}</code> applied to <code> {{ target }}</code>: <strong>{{ status }}</strong>
    </li>
    {% endfor %}
</ul>
<br>
{% endif %}

{% if unused_policies|length > 0 %}
The following policies exist but have not been applied anywhere:
<ul>
    {% for policy in unused_policies|sort %}
    <li>
        <code>{{ policy }}</code>
    </li>
    {% endfor %}
</ul>
<br>
{% endif %}
{% if active_policies|length > 0 %}
The following policies are applied to the given objects and have an <code>ACTIVE</code> status:
<ul>
    {% for (policy, target) in active_policies|sort(attribute="1,0") %}
    <li>
        <code>{{ policy }}</code> applied to <code>{{ target }}</code>
    </li>
    {% endfor %}
</ul>
{% endif %}""",
                {
                    "misapplied_policies": misapplied_policies,
                    "unused_policies": unused_policies,
                    "active_policies": active_policies,
                },
            ),
        )
