from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment, FutureGrant
from jetty_scorecard.util import render_string_template, truncated_database
import pandas as pd
import numpy as np


def create() -> Check:
    """Get coverage of future grants

    Look at dbs and schemas that have future grants applied.

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Future Grant Coverage",
        "See the future grant coverage across all databases and schemas.",
        (
            "Future grants help simplify access management by making it possible to"
            " define permissions and ownership of future database objects before they"
            " are created. Effectively using future grants helps reduce the risk of"
            " improper data access and allows expected permissions to be applied"
            " automatically.<br><br>This check excludes the SNOWFLAKE and"
            " SNOWFLAKE_SAMPLE_DATA databases, as well as INFORMATION_SCHEMA schemas."
        ),
        [
            (
                "https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html#future-grants-on-database-or-schema-objects",
                "Future Grants on Database or Schema Objects (Snowflake Documentation)",
            ),
            (
                "https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html",
                "GRANT <privileges> (Snowflake Documentation)",
            ),
        ],
        [FutureGrant],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Look at future grant coverage.


    Score is info if there are no future grants applied
    1-percent of db/schemas without future grants/2 if there are future grants.
    Unknown if there is no data

    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return None, "Unable to read future grant information"
    if len(env.future_grants) == 0:
        return (
            -1,
            (
                "There don't appear to be any future grants applied in your"
                " environment. You can add future grants with <code>GRANT {"
                " &lt;privileges&gt; | ALL } ON FUTURE &lt;object_type_plural&gt IN {"
                " DATABASE | SCHEMA } &lt;db_or_schema_name&gt;; </code>."
            ),
        )

    # Get schemas and their future grants
    all_schemas = pd.DataFrame(
        [
            {"schema": x.fqn(), "db": truncated_database(x.fqn())}
            for x in env.schemas
            if truncated_database(x.fqn())
            not in ('"SNOWFLAKE"', '"SNOWFLAKE_SAMPLE_DATA"')
            and x.name != "INFORMATION_SCHEMA"
        ]
    )

    future_grants_df = pd.DataFrame(
        [{"set_on": x.set_on, "asset_type": x.asset_type} for x in env.future_grants]
    )

    combined_tables = all_schemas.merge(
        future_grants_df, how="left", left_on="schema", right_on="set_on"
    ).merge(
        future_grants_df[future_grants_df["asset_type"] != "SCHEMA"],
        how="left",
        left_on="db",
        right_on="set_on",
    )

    combined_tables["coalesced_object_type"] = combined_tables[
        "asset_type_x"
    ].combine_first(combined_tables["asset_type_y"])

    schema_results = combined_tables.groupby("schema")["coalesced_object_type"].apply(
        set
    )
    schema_results = schema_results.where(schema_results != set({np.nan}), None)

    schema_with_future_grants = dict(schema_results[schema_results.notnull()])
    schema_without_future_grants = schema_results[schema_results.isnull()].index

    all_databases = pd.DataFrame(
        [
            {"db": x.fqn()}
            for x in env.databases
            if x.fqn() not in ('"SNOWFLAKE"', '"SNOWFLAKE_SAMPLE_DATA"')
        ]
    )
    combined_db_tables = all_databases.merge(
        future_grants_df[future_grants_df["asset_type"] == "SCHEMA"],
        how="left",
        left_on="db",
        right_on="set_on",
    ).drop_duplicates()

    db_with_future_grants = combined_db_tables[
        combined_db_tables["asset_type"].notnull()
    ]["db"].values
    db_without_future_grants = combined_db_tables[
        combined_db_tables["asset_type"].isnull()
    ]["db"].values

    # Calculate the score as 1 - percent of schemas/dbs without future grants / 2
    # It might seem weird, but my goal is to bound the score to .5-1. Not using
    # future grants isn't really an error, but a warning for low coverage may be
    # useful.
    score = (
        1
        - (
            1
            - (len(schema_with_future_grants) + len(db_with_future_grants))
            / (len(all_databases) + len(all_schemas))
        )
        / 2
    )
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
        """{% if db_without_future_grants|length > 0 %}
The following databases do not have future grants that will apply to new schemas:
<ul>
    {% for db in db_without_future_grants|sort %}
    <li><code>{{ db }}</code></li>
    {% endfor %}
</ul>
{% endif %}

{% if schema_without_future_grants|length > 0 %}
The following schemas do not have any future grants that will apply to new objects (granted at the schema or database
level):
<ul>
    {% for schema in schema_without_future_grants|sort %}
    <li><code>{{ schema }}</code></li>
    {% endfor %}
</ul>
{% endif %}


{% if db_with_future_grants|length > 0 %}
The following databases have future grants that will apply to new schemas:
<ul>
    {% for db in db_with_future_grants|sort %}
    <li><code>{{ db }}</code></li>
    {% endfor %}
</ul>
{% endif %}

{% if schema_with_future_grants|length > 0 %}
The following schemas have future grants that will apply to the new object types listed below (granted at the schema or
database level):
<ul>
    {% for (schema, object_types) in schema_with_future_grants.items()|sort(attribute='0') %}
    <li><code>{{ schema }}</code>
        <ul>
            {% for object_type in object_types|sort %}
            <li>{{ object_type }}</li>
            {% endfor %}
        </ul>
    </li>
    {% endfor %}
</ul>
{% endif %}""",
        {
            "schema_with_future_grants": schema_with_future_grants,
            "schema_without_future_grants": schema_without_future_grants,
            "db_with_future_grants": db_with_future_grants,
            "db_without_future_grants": db_without_future_grants,
        },
    )
    return score, details
