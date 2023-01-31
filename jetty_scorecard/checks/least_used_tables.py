from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, AccessHistory, Entity
from jetty_scorecard.util import render_string_template
import pandas as pd


def create() -> Check:
    """Find least-used tables from usage history

    Look at the tables that have been queried least frequently

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Least-Used Tables and Views",
        "Find the least frequently used tables and views",
        (
            "This check highlights the least commonly used tables and views from the"
            " last 90 days by leveraging the"
            " <code>SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY</code> table. These are the"
            " tables that are directly accessed, but if you'd also like to see the"
            " underlying tables accessed (in views, for example), you can look at"
            " <code>BASE_OBJECTS_ACCESSED</code> column of"
            " <code>SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY</code>."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/access-history.html#access-history",
                "Access History (Snowflake Documentation)",
            ),
        ],
        [AccessHistory, Entity],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Find least used tables.

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
        {"usage_count": "sum"}
    )

    all_tables = pd.DataFrame(
        [
            {"object": x.fqn(), "db": x.database, "schema": x.schema}
            for x in env.entities
            if x.entity_type in ("TABLE", "VIEW")
        ]
    )

    combined_tables = all_tables.merge(table_popularity.reset_index(), how="left")
    combined_tables["usage_count"] = combined_tables["usage_count"].fillna(0)
    filtered_tables = combined_tables[
        (combined_tables["schema"] != "INFORMATION_SCHEMA")
        & (~combined_tables["db"].isin(["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]))
    ]

    low_usage = (
        filtered_tables.sort_values(["usage_count", "object"], ascending=True)[
            ["object", "usage_count"]
        ]
        .head(10)
        .to_records(False)
    )

    details = render_string_template(
        """The least used tables and views in your account are:
<ul>
    {% for (table, usage_count) in low_usage %}
    <li>
        <code>{{ table }}</code> (used {{ "{:,.0f}".format(usage_count) }} {% if usage_count == 1 -%} time {% else %} times {% endif %}
    </li>
    {% endfor %}
</ul>""",
        {
            "low_usage": low_usage,
        },
    )
    return -2, details
