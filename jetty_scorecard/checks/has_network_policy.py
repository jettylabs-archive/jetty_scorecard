from __future__ import annotations

from jetty_scorecard.checks import Check
from jetty_scorecard.env import SnowflakeEnvironment
from jetty_scorecard.util import CustomQuery
from random import random


def create() -> Check:
    """Create a check for applied network policies.

    Network policies are a straightforward and effective way of limiting who
    has access to a Snowflake instance, and in what setting that access is
    allowed.

    Returns:
        Check: The network policy check.
    """
    return Check(
        "Active Network Policies",
        "Check for the presence of one or more active network policies",
        (
            "A Network policy is a simple and effective way to control who can access a"
            " Snowflake instance, and in what setting that access is allowed. A policy"
            " consists of an allowlist and/or denylist of IP addresses that is checked"
            " when a user logs in.<br><br>Once a policy is created, it can be applied"
            " at an account or user-specific level. "
        ),
        [
            (
                "https://docs.snowflake.com/en/user-guide/network-policies.html",
                "Network Policies (Snowflake Documentation)",
            ),
        ],
        [CustomQuery("SHOW PARAMETERS LIKE 'network_policy' IN ACCOUNT;")],
        _runner,
    )


def _runner(env: SnowflakeEnvironment) -> tuple[float, str]:
    """Check the environment for an active network policy

    If there is an active policy, the score is 1, if not it's .49 (fail).
    If there is no information, it is None

    Returns:
        float: Score
        str: Details
    """
    if env.has_network_policy is None:
        return (None, "Unknown network policy status")
    elif env.has_network_policy:
        return (
            1,
            (
                "There are one or more active network policies. You can use the query"
                " provided to see how policies are being applied. You can also run"
                " <code>SHOW NETWORK POLICIES</code> to see all existing policies"
                " (active or not)."
            ),
        )
    else:
        return (
            0.49,
            """<p>There are no active network policies in your Snowflake account. Refer to the \
Snowflake documentation for help setting up and applying your first policy. If \
your organization uses a VPN, creating a policy to limit use to users on the VPN \
would be a great place to start.<br><br>
<strong>Note:</strong> When setting up an account-wide \
network policy, be sure to have user-specific polices already in place for any \
users that might need exceptions. This could include accounts for internal or \
external tools that depend on Snowflake. To check for potential exceptions, \
you could run a query like this:<br>

<div style="margin-left: 20px">
<code>SELECT user_name, <br>
    &ensp;&ensp;client_ip, <br>
    &ensp;&ensp;MAX(event_timestamp) as last_login_from_ip <br>
FROM table(snowflake.information_schema.login_history()) <br>
GROUP BY user_name, client_ip;</code>
</div></p>
""",
        )
