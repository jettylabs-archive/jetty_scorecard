from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, LoginHistory, User, AccessHistory
from jetty_scorecard.util import render_string_template


def create() -> Check:
    """Create a check for unused users

    This checks for users that haven't been used within the last 7 and 90 days.
    It excludes the Snowflake user.

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Inactive Users",
        "Check for non-disabled users that haven't used Snowflake in a while",
        (
            "When a users stops using Snowflake, their account can be dropped or"
            " disabled. If they are not, those unused accounts represent a possible"
            " point of entry into the system. This check looks for non-disabled users"
            " that haven't logged in for the last 7 days or haven't accessed a table in"
            " the last 90 days (Enterprise edition only).<br><br>Note: This check"
            " excludes the SNOWFLAKE user used (by permission) by Snowflake support to"
            " diagnose problems. This account can also safely be disabled or removed."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/admin-user-management.html#disabling-enabling-a-user",
                "Disabling / Enabling a User (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/user-guide/admin-user-management.html#dropping-a-user",
                "Dropping a User (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/sql-reference/sql/drop-user.html",
                "DROP USER (Snowflake Documentation)",
            ),
            (
                "https://community.snowflake.com/s/article/HowTo-How-to-disable-the-SNOWFLAKE-user-that-is-created-by-default",
                (
                    "HowTo: Disable the SNOWFLAKE user from your Snowflake account"
                    " (Snowflake Knowledge Base"
                ),
            ),
            (
                "https://community.snowflake.com/s/article/Automatically-Disable-Users-Who-Have-Not-Logged-Into-Snowflake-For-A-Defined-Period-Of-Time",
                (
                    "Automatically Disable Users Who Have Not Logged In For A Defined"
                    " Period Of Time (Snowflake Knowledge Base)"
                ),
            ),
        ],
        [LoginHistory, User, AccessHistory],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for inactive users.

    Score is .75 if any users haven't logged in in the last week
    Score is .25 if any users haven't accessed a table in the last 90 days

    If there is no information, it is None


    Returns:
        float: Score
        str: Details
    """
    if not env.has_data or env.login_history is None:
        return None, "Unable to check login history"

    non_disabled_users = set(
        [x.name for x in env.users if not x.disabled and not x.name == "SNOWFLAKE"]
    )

    have_logged_in = set([x.user for x in env.login_history if x.success])
    no_login = [x for x in non_disabled_users if x not in have_logged_in]

    if env.access_history is not None:
        have_accessed = set(env.access_history.tables["user"])
        no_access = [x for x in non_disabled_users if x not in have_accessed]
    else:
        no_access = None

    score = 1

    if len(no_login) > 0:
        score = 0.75
    if no_access is not None and len(no_access) > 0:
        score = 0.25

    details = render_string_template(
        """{% if no_login|length > 0 %}
The following users have not logged in in the last 7 days:
<ul>
    {% for user in no_login %}
    <li>{{ user }}</li>
    {% endfor %}
</ul>
{% endif %}
{% if no_access is not none and no_access|length > 0 %}
The following users have not accessed a table in the last 90 days:
<ul>
    {% for user in no_access %}
    <li>{{ user }}</li>
    {% endfor %}
</ul>
{% endif %}
{% if no_login|length > 0 or (no_access is not none and no_access|length > 0) %}
Consider disabling or dropping the users listed above.
{% else %}
All of your non-disabled users have logged in in the last 7 days.
{% endif %}
    """,
        {
            "no_access": no_access,
            "no_login": no_login,
        },
    )
    return score, details
