from jetty_scorecard import env
from jetty_scorecard.checks import (
    backup_account_admin,
    has_network_policy,
    inaccessible_objects,
    shadow_future_grants,
    overuse_of_admin_roles,
    password_only_login,
    inactive_users,
    managed_access_schemas,
    most_used_tables,
    most_used_columns,
    least_used_tables,
    future_grant_coverage,
    most_accessible_objects,
    least_accessible_objects,
    active_masking_policies,
    active_row_access_policies,
    potentially_sensitive_columns,
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
        inactive_users.create(),
        managed_access_schemas.create(),
        inaccessible_objects.create(),
        most_used_tables.create(),
        most_used_columns.create(),
        least_used_tables.create(),
        future_grant_coverage.create(),
        most_accessible_objects.create(),
        least_accessible_objects.create(),
        active_masking_policies.create(),
        active_row_access_policies.create(),
        potentially_sensitive_columns.create(),
    ]

    for check in check_list:
        env.register_check(check)
