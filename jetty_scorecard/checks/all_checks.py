from jetty_scorecard import env
from jetty_scorecard.checks import (
    backup_account_admin,
    test_check,
    has_network_policy,
    shadow_future_grants,
)


def register(env: env.SnowflakeEnvironment):
    """Register the checks specified in the following list

    Args:
        env (env.SnowflakeEnvironment): Environment to register the checks with
    """
    check_list = [
        test_check.create(),
        backup_account_admin.create(),
        shadow_future_grants.create(),
        has_network_policy.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
    ]

    for check in check_list:
        env.register_check(check)
