from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, LoginHistory
from jetty_scorecard.util import render_check_template


def create() -> Check:
    """Create a check for password-only logins

    Ideally, users login with SSO, MFA, or maybe a private key. This checks
    for users logging in with a password and no MFA

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Password-Only Logins",
        "Check for users looging in with a password and no MFA",
        (
            "Snowflake supports several authentication methods, and recommends that"
            " username and password (particularly without MFA) is only used when"
            " absolutely necessary. This check looks at the percentage of successful"
            " logins over the past week that used password-only authentication. This is"
            " particularly important for users with administrator roles."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/admin-user-management.html#best-practices-for-password-policies-and-passwords",
                (
                    "Best Practices for Password Policies and Passwords (Snowflake"
                    " Documentation)"
                ),
            ),
            (
                "https://docs.snowflake.com/en/user-guide/security-mfa.html#managing-mfa-for-an-account-and-users",
                "Multi-Factor Authentication (MFA) (Snowflake Documentation)",
            ),
            (
                "https://community.snowflake.com/s/article/Snowflake-Security-Overview-and-Best-Practices#:~:text=Authentication%20best%20practices",
                (
                    "Snowflake Security Overview and Best Practices (Snowflake"
                    " Knowledge Base)"
                ),
            ),
            (
                "https://docs.snowflake.com/en/sql-reference/sql/create-password-policy.html",
                "CREATE PASSWORD POLICY (Snowflake Documentation)",
            ),
        ],
        [LoginHistory],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for password only logins.

    Score is percent of no-password-only logins in the last week

    If there is no information, it is None

    Returns:
        float: Score
        str: Details
    """
    if not env.has_data or env.login_history is None:
        return (None, "Unable to check login history")

    total_logins = len([1 for x in env.login_history if x.success])
    password_only_logins = [
        x
        for x in env.login_history
        if x.success
        and x.first_authentication_factor == "PASSWORD"
        and x.second_authentication_factor is None
    ]
    percent_password_only = len(password_only_logins) / total_logins * 100
    password_only_users = set([x.user for x in password_only_logins])

    score = 1 - (len(password_only_logins) / total_logins)

    details = render_check_template(
        "password_only_login.html.jinja",
        {
            "percent_password_only": percent_password_only,
            "password_only_users": password_only_users,
        },
    )
    return score, details
