from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, AccessHistory
from jetty_scorecard.util import render_string_template


def create() -> Check:
    """Find most-used tables from usage history

    Look at the tables that have been queried most frequently and by the most users

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Most-Used Tables and Views",
        "Find the most frequently and widely used tables and views",
        (
            "This check highlights commonly used tables and views from the last 90 days"
            " by leveraging the <code>SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY</code>"
            " table. These are the tables that are directly accessed, but if you'd also"
            " like to see the underlying tables accessed (in views, for example), you"
            " can look at <code>BASE_OBJECTS_ACCESSED</code> column of"
            " <code>SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY</code>."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/access-history.html#access-history",
                "Access History (Snowflake Documentation)",
            ),
        ],
        [AccessHistory],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Find most used tables.

    Score is insight if there is information, info if there is none

    Returns:
        float: Score
        str: Details
    """
    if env.access_history is None:
        return (
            -1,
            (
                "The <code>ACCESS_HISTORY</code> table is available as part of"
                " Snowflake Enterprise Edition. It provides fantastic insight into what"
                " data has been queried or modified, down to a column level. It also"
                " provides information, not just about what data has been accessed,"
                " but, in the case of views, for example, what are the underlying"
                " resources referenced by the view."
            ),
        )

    table_popularity = env.access_history.tables.groupby("object").agg(
        {"user": "count", "usage_count": "sum"}
    )
    top_usage = (
        table_popularity.sort_values(["usage_count", "user"], ascending=False)
        .head(10)
        .to_records()
    )
    most_users = (
        table_popularity.sort_values(["user", "usage_count"], ascending=False)
        .head(10)
        .to_records()
    )

    details = render_string_template(
        """The most frequently used tables and views in your account are:
<ul>
    {% for (table, user_count, usage_count) in top_usage %}
    <li>
        <code>{{ table }}</code> (used {{ "{:,}".format(usage_count) }} {% if usage_count == 1 -%} time {% else %} times {% endif %}
        by {{ "{:,}".format(user_count) }} {% if user_count == 1 -%} user {% else %} users {% endif %})
    </li>
    {% endfor %}
</ul>

The most widely used tables and views in your account are:
<ul>
    {% for (table, user_count, usage_count) in most_users %}
    <li>
        <code>{{ table }}</code> (used {{ "{:,}".format(usage_count) }} {% if usage_count == 1 -%} time {% else %} times {% endif %}
        by {{ "{:,}".format(user_count) }} {% if user_count == 1 -%} user {% else %} users {% endif %})
    </li>
    {% endfor %}
</ul>""",
        {
            "top_usage": top_usage,
            "most_users": most_users,
        },
    )
    return -2, details
