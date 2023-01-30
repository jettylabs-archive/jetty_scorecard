from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, RoleGrant, RoleGrantNodeType, User
from jetty_scorecard.util import render_string_template
import networkx as nx


def create() -> Check:
    """Create number of admins.

    It is best to have a relatively small number of admins. This checks to see
    if you have no more than max(3, num_users/10) admins.

    Returns:
        Check: The Check instance.
    """
    return Check(
        "Overuse of Admin Roles",
        "Checks the number of users with administrator roles",
        (
            "Only a limited number of users should be granted Snowflake Administrator"
            " roles, particularly <code>ACCOUNTADMIN</code> and"
            " <code>SECURITYADMIN</code> (which can grant the <code>ACCOUNTADMIN</code>"
            " role). This check looks at the percentage of users with one of these two"
            " roles. While the optimum number of administrators will vary between"
            ' organizations, this check sets a "passing" threshold of the larger of 3'
            " and 10% of the total number of account users."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-considerations.html",
                "Access Control Considerations (Snowflake Documentation)",
            ),
        ],
        [RoleGrant, User],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for a high number of admins.

    If there are fewer than 30 account users:
        If there are <=3 admins, the score is 1
        Else, it's .49
    Else the score is 1-(percent of account users that are admins)

    If there is no information, it is None

    Looking only at <code>ACCOUNTADMIN</code> and <code>SECURITYADMIN</code> roles.
    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return (None, "Unable to calculate number of admins")

    account_admins = [
        x[0]
        for x in nx.descendants(
            env.role_graph, ("ACCOUNTADMIN", RoleGrantNodeType.ROLE)
        )
        if x[1] == RoleGrantNodeType.USER
    ]

    security_admins = [
        x[0]
        for x in nx.descendants(
            env.role_graph, ("SECURITYADMIN", RoleGrantNodeType.ROLE)
        )
        if x[1] == RoleGrantNodeType.USER
    ]

    admin_set = set(account_admins + security_admins)

    num_users = len(env.users)

    if num_users <= 30 and len(admin_set) <= 3:
        score = 1
    elif num_users <= 30 and len(admin_set) > 3:
        score = 0.49
    else:
        score = 1 - (len(admin_set) / num_users)
        if score > 0.9:
            score = 1

    return (
        score,
        render_string_template(
            """There are {{ admin_set|length }} users in your account with the <code>ACCOUNTADMIN</code> or <code>SECURITYADMIN</code> roles:
<ul>
    <li>
        ACCOUNTADMIN
        <ul>
            {% for (user) in account_admins|sort %}
            <li>
                {{ user }}
            </li>
            {% endfor %}
        </ul>
    </li>
    <li>
        SECURITYADMIN
        <ul>
            {% for (user) in security_admins|sort %}
            <li>
                {{ user }}
            </li>
            {% endfor %}
        </ul>
    </li>
</ul>
""",
            {
                "admin_set": admin_set,
                "account_admins": account_admins,
                "security_admins": security_admins,
            },
        ),
    )
