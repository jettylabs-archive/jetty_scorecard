from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.checks.common import user_object_access
from jetty_scorecard.env import (
    SnowflakeEnvironment,
    PrivilegeGrant,
    RoleGrant,
    Column,
    User,
)
from jetty_scorecard.util import render_check_template, truncated_table
import pandas as pd


sensitive_terms = [
    "email",
    "e-mail",
    "cell",
    "fax",
    "phone",
    "^ssn$",
    "birth",
    "born",
    "^age$",
    "^sex$",
    "gender",
    "mailing",
    "address",
    "street",
    "^zip",
    "^cc",
    "credit_card",
    "license_plate",
    "^lat",
    "^long",
    "^ip",
    "_ip$",
    "first_name",
    "last_name",
    "firstname",
    "lastname",
    "^name$",
    "^amount$",
    "^paid$",
]

safe_terms = ["hashed", "safe", "masked"]


def create() -> Check:
    """Create a check for sensitive, accessible

    Look for potentially sensitive column names that are widely visible

    Returns:
        Check: instance of Check.
    """
    return Check(
        "Potentially Sensitive Columns",
        "Find potentially sensitive columns that are widely accessible",
        (
            "This check looks for potentially sensitive column names (things like"
            " email, first_name, etc.) that are widely visible (here, we'll use a"
            " threshold of the greater of 3 users or 10% of all users). This check"
            " excludes columns that also include words like hashed and masked, and"
            " excludes columns that have an active masking policy applied to them (if"
            " Scorecard has permission to see masking policy references).<br><br>If you"
            " find columns that should be better protected, Snowflake provides several"
            " tools to make this process fast and scalable, including flexible role"
            " hierarchies, automatic data classification, and tag-based data"
            " masking.<br><br>This check relies on simple pattern matching, so some"
            " columns may not be sensitive, and some sensitive columns will be missed."
            " It is intended to provide a starting point in ensuring that"
            " sensitive data is appropriately protected.<br><br>Note: This check"
            " excludes access granted via database roles (a preview feature in"
            " Snowflake), and may not take all ACCOUNTADMIN permissions into account."
            " It also ignores the <code>SNOWFLAKE</code> and"
            " <code>SNOWFLAKE_SAMPLE_DATA</code> databases and"
            " <code>INFORMATION_SCHEMA</code> schemas."
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/security-access-control-overview.html",
                "Overview of Access Control (Snowflake Documentation)",
            ),
            (
                "https://www.snowflake.com/blog/protect-sensitive-data-tag-based-masking/",
                (
                    "Protect Your Sensitive Data Better than Ever with Tag-Based"
                    " Masking (Snowflake Blog)"
                ),
            ),
            (
                "https://docs.snowflake.com/en/user-guide/tag-based-masking-policies.html",
                "Tag-based Masking Policies (Snowflake Documentation",
            ),
            (
                "https://docs.snowflake.com/en/user-guide/governance-classify.html",
                "Data Classification (Snowflake Documentation)",
            ),
        ],
        [PrivilegeGrant, RoleGrant, Column, User],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check for most accessible objects

    This looks for potentially sensitive column names that are widely visible,
    meaning that max(3, 10% of all users) have some permissions on it

    Score is .89 if violations are found, 1 if not. The score is high because it
    is totally heuristic based, and may come up with some wonky results.

    If there is no information, it is None

    Returns:
        float: Score
        str: Details
    """
    if not env.has_data:
        return None, "Unable to read column names."

    columns = pd.DataFrame(
        [
            {"column_name": x.name, "fqn": x.fqn()}
            for x in env.columns
            if x.database not in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA")
            and x.schema not in ("INFORMATION_SCHEMA")
        ]
    )

    risky_columns = columns[
        columns["column_name"].str.lower().str.contains("|".join(sensitive_terms))
        & ~columns["column_name"].str.lower().str.contains("hashed|masked|safe")
    ].copy()
    risky_columns["table"] = risky_columns["fqn"].apply(truncated_table)

    # Exclude those with a masking policy applied, if possible
    if env.masking_policy_references is not None:
        masked_columns = pd.DataFrame(
            [{"column_name": x.target_fqn} for x in env.masking_policy_references]
        )
        risky_columns = risky_columns[
            ~risky_columns["column_name"].isin(masked_columns)
        ]

    # Get number of non-disabled users
    num_users = len([x for x in env.users if not x.disabled])

    # Get "widely accessible" threshold
    threshold = int(max(3, num_users / 10))

    # get access level for each table
    access = user_object_access(env)
    # each object with a set of users that have access to it
    set_list = access.groupby("object")["user"].apply(set)
    access_counts = set_list.str.len().to_frame().reset_index()

    # this is an inner join, meaning we're ignoring any tables that we don't have info for
    combined_table = risky_columns.merge(
        access_counts, left_on="table", right_on="object"
    )

    results = combined_table[combined_table["user"] > threshold][
        ["fqn", "user"]
    ].to_records(False)

    if len(results) == 0:
        score = 1
        details = (
            "This check didn't detect any potentially sensitive"
            f" columns that accessible to more than {threshold:,.0f}."
            " Try running your own checks that include common column names and"
            " relevant organizational context to verify these results."
        )

    else:
        score = 0.89
        details = render_check_template(
            "potentially_sensitive_columns.html.jinja",
            {
                "threshold": threshold,
                "results": results[:50],
                "total_result_count": len(results),
            },
        )

    return score, details
