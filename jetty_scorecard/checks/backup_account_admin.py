from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, RoleGrant, RoleGrantNodeType
from jetty_scorecard.util import render_check_template
import networkx as nx


def create() -> Check:
    """Create a check for backup account admins.

    Having a single account admin is risky, so this check ensures that
    you have a backup.

    Returns:
        Check: The account admin backup check.
    """
    return Check(
        "Backup Account Admin",
        "Check for a backup account administrator",
        (
            "Each account has a single user automatically assigned the ACCOUNTADMIN"
            " role. It is recommended that at least one other user be assigned the"
            " ACCOUNTADMIN role so that account-level tasks can be performed even if"
            " the default ACCOUNTADMIN is unable to log in."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-configure.html#designating-additional-users-as-account-administrators",
                (
                    "Designating Additional Users as Account Administrators (Snowflake"
                    " Documentation)"
                ),
            ),
        ],
        [RoleGrant],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for a backup account administrator.

    If there are two or more users with the ACCOUNTADMIN role,
    the score is 1, if not it's .49 (fail).

    If there is no information, it is None

    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return (None, "Unable to check for a backup account administrator")

    account_admins = [
        x[0]
        for x in nx.descendants(
            env.role_graph, ("ACCOUNTADMIN", RoleGrantNodeType.ROLE)
        )
        if x[1] == RoleGrantNodeType.USER
    ]

    if len(account_admins) > 1:
        return (
            1,
            render_check_template(
                "backup_account_admin.html.jinja",
                {"account_admins": account_admins},
            ),
        )
    else:
        return (
            0.49,
            (
                f"{account_admins[0]} appears to be the only <code>ACCOUNTADMIN</code>"
                " in your account. Assign this role to another user with <code>GRANT"
                " ROLE ACCOUNTADMIN TO USER &lt;username&gt;</code>."
            ),
        )
