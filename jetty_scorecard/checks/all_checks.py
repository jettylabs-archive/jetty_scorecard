from jetty_scorecard import env
from jetty_scorecard.checks import test_check


def register(env: env.SnowflakeEnvironment):
    """Register the checks specified in the following list

    Args:
        env (env.SnowflakeEnvironment): Environment to register the checks with
    """
    check_list = [
        test_check.create(),
        test_check.create(),
        test_check.create(),
        test_check.create(),
    ]

    for check in check_list:
        env.register_check(check)
