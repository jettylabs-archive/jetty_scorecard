from jetty_scorecard import env
from jetty_scorecard.checks import (
    backup_account_admin,
    has_network_policy,
    shadow_future_grants,
    overuse_of_admin_roles,
    password_only_login,
)


def register(env: env.SnowflakeEnvironment):
    """Register the checks specified in the following list

    Args:
        env (env.SnowflakeEnvironment): Environment to register the checks with
    """
    check_list = [
        overuse_of_admin_roles.create(),
        backup_account_admin.create(),
        shadow_future_grants.create(),
        has_network_policy.create(),
        password_only_login.create(),
    ]

    for check in check_list:
        env.register_check(check)
