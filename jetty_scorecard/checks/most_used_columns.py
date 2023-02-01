from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, AccessHistory
from jetty_scorecard.util import render_string_template


def create() -> Check:
    """Find most-used columns from usage history

    Look at the columns that have been queried most frequently and by the most users

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Most-Used Columns",
        "Find the most frequently and widely used columns",
        (
            "This check highlights commonly used columns from the last 90 days by"
            " leveraging the <code>SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY</code> table."
            " These are the columns that are directly accessed, but if you'd also like"
            " to see the underlying columns accessed (in views, for example), you can"
            " look at <code>BASE_OBJECTS_ACCESSED</code> column of"
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
    """Find most used columns.

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

    column_popularity = env.access_history.columns.groupby("object").agg(
        {"user": "count", "usage_count": "sum"}
    )
    top_usage = (
        column_popularity.sort_values(["usage_count", "user"], ascending=False)
        .head(10)
        .to_records()
    )
    most_users = (
        column_popularity.sort_values(["user", "usage_count"], ascending=False)
        .head(10)
        .to_records()
    )

    details = render_string_template(
        """The most frequently used columns in your account are:
<ul>
    {% for (column, user_count, usage_count) in top_usage %}
    <li>
        <code>{{ column }}</code> (used {{ "{:,.0f}".format(usage_count) }} {% if usage_count == 1 -%} time {% else %} times {% endif %}
        by {{ "{:,.0f}".format(user_count) }} {% if user_count == 1 -%} user {% else %} users {% endif %})
    </li>
    {% endfor %}
</ul>

The most widely used columns in your account are:
<ul>
    {% for (column, user_count, usage_count) in most_users %}
    <li>
        <code>{{ column }}</code> (used {{ "{:,.0f}".format(usage_count) }} {% if usage_count == 1 -%} time {% else %} times {% endif %}
        by {{ "{:,.0f}".format(user_count) }} {% if user_count == 1 -%} user {% else %} users {% endif %})
    </li>
    {% endfor %}
</ul>""",
        {
            "top_usage": top_usage,
            "most_users": most_users,
        },
    )
    return -2, details
