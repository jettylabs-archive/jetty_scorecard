"""The module used to interact with Snowflake and Snowflake metadata"""
from __future__ import annotations

import pandas as pd
from tqdm import tqdm
import json
from datetime import datetime
import itertools
from jetty_scorecard import util, checks
from jinja2 import PackageLoader, Environment
from snowflake.connector import SnowflakeConnection, DictCursor
import snowflake.connector
from copy import deepcopy
from jetty_scorecard.cli import TextFormat
from jetty_scorecard.util import Queryable
from enum import Enum, auto
import networkx as nx


class SnowflakeEnvironment:
    """The main class for interacting with Snowflake.

    Handles the fetching of environment metadata and the running of checks
    on that metadata.
    All attributes except max_workers are initialized as None or []. The
    values are then populated by establishing a connection (connect() method),
    fetching the environment (fetch_environment()), registering checks
    (register_checks()), and running checks (run_checks()).


    Attributes:
        max_workers: Integer value specifying the number of allowed concurrent
          requests to Snowflake.
        databases: List of Database instances
        schemas: List of Schema instances
        entities: List of Entity instances
        columns: List of Column instances
        users: List of User instances
        roles: List of Role instances
        role_grants: List of RoleGrant instances describing what roles have
          been granted to what users/roles
        privilege_grants: List of PrivilegeGrant instances describing what
          privileges have been granted to roles. This includes privileges on
          databases, schemas, and tables
        row_access_policies: List of RowAccessPolicy instances. Only available
          on Snowflake Enterprise edition.
        masking_policies: List of MaskingPolicy instances. Only available on
          Snowflake Enterprise edition.
        future_grants: List of FutureGrant instances
        login_history: List of LoginHistory instances, describing individual
          login attempts
        access_history: AccessHistory instance, containing information about
          tables and views accessed by each user. Only available on Snowflake
          Enterprise edition. Requires permissions on account_usage tables.
        masking_policy_references: List of MaskingPolicyReference instances
          specifying information about specific applications of masking
          policies. Only available on Snowflake Enterprise edition. Requires
          permissions on account_usage tables.
        row_access_policy_references: List of RowAccessPolicyReference
          instances specifying information about specific applications of
          row access policies. Only available on Snowflake Enterprise edition.
          Requires permissions on account_usage tables.
        has_network_policy: Boolean flag of whether the account has any
          network policies
        is_enterprise_or_higher: Boolean flag of whether the account is
          enterprise level or higher
        conn: A SnowflakeConnection used to build out the environment
        checks: A list of checks to be run in the environment
    """

    databases: list[Database] | None
    schemas: list[Schema] | None
    entities: list[Entity] | None
    columns: list[Column] | None
    users: list[User] | None
    roles: list[Role] | None
    role_grants: list[RoleGrant] | None
    privilege_grants: list[PrivilegeGrant] | None
    row_access_policies: list[RowAccessPolicy] | None
    masking_policies: list[MaskingPolicy] | None
    future_grants: list[FutureGrant] | None
    login_history: list[LoginHistory] | None
    access_history: AccessHistory | None
    masking_policy_references: list[MaskingPolicyReference] | None
    row_access_policy_references: list[RowAccessPolicyReference] | None
    has_network_policy: bool | None
    is_enterprise_or_higher: bool | None
    conn: SnowflakeConnection | None
    max_workers: int
    checks: list[checks.Check]
    _role_graph: nx.DiGraph

    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.databases = None
        self.schemas = None
        self.entities = None
        self.columns = None
        self.users = None
        self.roles = None
        self.role_grants = None
        self.privilege_grants = None
        self.row_access_policies = None
        self.masking_policies = None
        self.future_grants = None
        self.login_history = None
        self.access_history = None
        self.has_network_policy = None
        self.is_enterprise_or_higher = None
        self.conn = None
        self.checks = []
        self.masking_policy_references = None
        self.row_access_policy_references = None
        self._role_graph = None

    def copy(self) -> SnowflakeEnvironment:
        """Copy an existing SnowflakeEnvironment

        Create a copy of self, excluding the value in self.conn. A new
        self.conn value can be set with the connect() method.

        Returns:
            A copy of self
        """
        env = SnowflakeEnvironment(self.max_workers)
        env.databases = deepcopy(self.databases)
        env.schemas = deepcopy(self.schemas)
        env.entities = deepcopy(self.entities)
        env.columns = deepcopy(self.columns)
        env.users = deepcopy(self.users)
        env.roles = deepcopy(self.roles)
        env.role_grants = deepcopy(self.role_grants)
        env.privilege_grants = deepcopy(self.privilege_grants)
        env.row_access_policies = deepcopy(self.row_access_policies)
        env.masking_policies = deepcopy(self.masking_policies)
        env.future_grants = deepcopy(self.future_grants)
        env.login_history = deepcopy(self.login_history)
        env.access_history = deepcopy(self.access_history)
        env.has_network_policy = deepcopy(self.has_network_policy)
        env.is_enterprise_or_higher = deepcopy(self.is_enterprise_or_higher)
        env.conn = None
        env._role_graph = None
        env.checks = deepcopy(self.checks)
        env.masking_policy_references = deepcopy(self.masking_policy_references)
        env.row_access_policy_references = deepcopy(self.row_access_policy_references)
        return env

    def connect(self, credentials):
        """Connect to Snowflake

        Creates a Snowflake connection via Snowflake's python library.

        Args:
            credentials (dict): A dictionary containing the appropriate keys
              (specified in the Snowflake docs: https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-to-snowflake)

        Returns:
            None
        """
        self.conn = snowflake.connector.connect(**credentials)

    def is_ok(self) -> bool:
        """Check the connection to Snowflake

        Returns:
            True if the connection to Snowflake is healthy, False otherwise
        """
        try:
            with self.conn.cursor() as cur:
                statement = "SELECT 1"
                cur.execute(statement)
        except:
            return False

        return True

    @property
    def role_graph(self) -> nx.DiGraph | None:
        if not self.has_data:
            return None
        if self._role_graph is None:
            DG = nx.DiGraph()
            DG.add_edges_from(
                [
                    (
                        (r.role, RoleGrantNodeType.ROLE),
                        (
                            r.grantee,
                            RoleGrantNodeType.USER
                            if r.grantee_type == "USER"
                            else RoleGrantNodeType.ROLE,
                        ),
                    )
                    for r in self.role_grants
                ]
            )
            self._role_graph = DG
        return self._role_graph

    def run_checks(self):
        """Run all checks in the environment

        Runs all checks that have been registered in the environment

        Returns:
            None
        """
        print("\nRunning checks")
        for check in tqdm(self.checks):
            check.run(self)

    @property
    def has_data(self) -> bool:
        """Check if the environment has data

        Returns:
            True if the environment has data, False otherwise
        """
        return self.databases is not None

    @property
    def num_pass_checks(self) -> int:
        """Number of checks with a passing grade

        Returns:
            Number of checks with a passing grade
        """
        return len(
            [True for check in self.checks if check.status == checks.CheckStatus.PASS]
        )

    @property
    def num_warn_checks(self) -> int:
        """Number of checks with a warning grade

        Returns:
            Number of checks with a warning grade
        """
        return len(
            [True for check in self.checks if check.status == checks.CheckStatus.WARN]
        )

    @property
    def num_fail_checks(self) -> int:
        """Number of checks with a failing grade

        Returns:
            Number of checks with a failing grade
        """
        return len(
            [True for check in self.checks if check.status == checks.CheckStatus.FAIL]
        )

    @property
    def num_info_checks(self) -> int:
        """Number of checks returning info, but no grade

        This could include checks that can't run because they're not on
        an enterprise edition Snowflake instance. The details provided could
        still be helpful, but no actual test was run.

        Returns:
            Number of checks returning info
        """
        return len(
            [True for check in self.checks if check.status == checks.CheckStatus.INFO]
        )

    @property
    def num_insight_checks(self) -> int:
        """Number of checks returning insight, but no grade

        This could include checks that are designed to show useful information,
        like the most popular tables or columns, but aren't a pass/fail sort of
        evaluation.

        Returns:
            Number of checks returning an insight result
        """
        return len(
            [
                True
                for check in self.checks
                if check.status == checks.CheckStatus.INSIGHT
            ]
        )

    @property
    def num_unknown_checks(self) -> int:
        """Number of checks with an unknown grade

        Unknown grades include when a check isn't successfully run or
        configured.

        Returns:
            Number of checks with an unknown grade
        """
        return len(
            [
                True
                for check in self.checks
                if check.status == checks.CheckStatus.UNKNOWN
            ]
        )

    @property
    def score(self) -> float:
        """The score from all the checks run in the environment

        This score averages the scores of each individual check, excluding
        Information, Insight, and Unknown check statuses.

        Returns:
            The averaged score from all the scored checks
        """
        check_scores = [
            x.score for x in self.checks if x.score is not None and x.score >= 0
        ]
        if len(check_scores) == 0:
            return None
        return sum(check_scores) / len(check_scores)

    @property
    def grade(self) -> str:
        """The grade resulting from self.score

        Returns:
            The grade resulting from all scored checks
        """
        if self.score is None:
            return "?"
        return util.percentage_to_grade(self.score, 0.25, 1)

    def register_check(self, check: checks.Check):
        """Register a check in the environment

        This must be done before running self.run_checks()

        Args:
            check: The check to register

        Returns:
            None
        """
        self.checks.append(check)

    def check_is_enterprise_or_higher(self):
        """Check if the environment is enterprise or higher

        Returns:
            True if the environment is enterprise or higher, False otherwise
        """
        if self.is_enterprise_or_higher:
            return True

        try:
            with self.conn.cursor() as cur:
                cur.execute("SHOW ROW ACCESS POLICIES")
        except Exception as e:
            if type(
                e
            ).__name__ == "ProgrammingError" and "Unsupported feature" in "".join(
                e.args
            ):
                self.is_enterprise_or_higher = False
            else:
                raise e
        else:
            self.is_enterprise_or_higher = True

    def check_network_policy(self):
        """Check if the environment has at least one network policy

        Returns:
            True if the environment has a network policy, False otherwise
        """
        if self.has_network_policy:
            return True

        with self.conn.cursor() as cur:
            self.has_network_policy = cur.execute("SHOW NETWORK POLICIES;").rowcount > 0

    def fetch_databases(self):
        """Fetch database metadata from Snowflake

        This fetches the Database metadata and saves it as an instance variable

        Returns:
            None
        """
        databases = []
        with self.conn.cursor(DictCursor) as cur:
            statement = "SHOW DATABASES"
            for row in cur.execute(statement):
                databases.append(Database.from_row(row))

        self.databases = databases

    def fetch_users(self):
        """Fetch user metadata from Snowflake

        This fetches the User metadata and saves it as an instance variable

        Returns:
            None
        """
        users = []
        with self.conn.cursor(DictCursor) as cur:
            statement = "SHOW USERS"
        users = []
        with self.conn.cursor(DictCursor) as cur:
            statement = "SHOW USERS"
            for row in cur.execute(statement):
                users.append(User.from_row(row))
        self.users = users

    def fetch_masking_policies(self):
        """Fetch data masking policies

        This fetches the data masking policies and saves it as an instance
        variable. It should only be run if the Snowflake account is enterprise
        edition or higher.

        Returns:
            None
        """
        masking_policies = []
        with self.conn.cursor(DictCursor) as cur:
            statement = "SHOW MASKING POLICIES"
            for row in cur.execute(statement):
                masking_policies.append(MaskingPolicy.from_row(row))
        self.masking_policies = masking_policies

    def fetch_row_access_policies(self):
        """Fetch row access policies

        This fetches the row access policies and saves it as an instance
        variable. It should only be run if the Snowflake account is enterprise
        edition or higher.

        Returns:
            None
        """
        row_access_policies = []
        with self.conn.cursor(DictCursor) as cur:
            statement = "SHOW ROW ACCESS POLICIES"
            for row in cur.execute(statement):
                row_access_policies.append(RowAccessPolicy.from_row(row))
        self.row_access_policies = row_access_policies

    def fetch_login_history(self):
        """Fetch login history from the past week

        This method will raise an exception if the Snowflake user doesn't
        have USAGE permissions on the snowflake.information_schema.login_history()
        function.

        Returns:
            None
        """
        login_history = []
        with self.conn.cursor(DictCursor) as cur:
            statement = (
                "SELECT * FROM table(snowflake.information_schema.login_history())"
            )
            for row in cur.execute(statement):
                login_history.append(LoginHistory.from_row(row))
        self.login_history = login_history

    def fetch_access_history(self):
        """Fetch access history from the past 90 days

        Sets the access_history instance variable. This method will raise
        an exception if the Snowflake user doesn't have permissions on the
        snowflake.account_usage.access_history table.

        Returns:
            None
        """
        with self.conn.cursor(DictCursor) as cur:
            statement = (
                "SELECT user_name, direct_objects_accessed FROM"
                " snowflake.account_usage.access_history WHERE query_start_time >"
                " DATEADD('DAY', -90, CURRENT_TIMESTAMP())"
            )
            rows = cur.execute(statement).fetchall()
        self.access_history = AccessHistory.from_rows(rows)

    def fetch_policy_references(self):
        """Fetch policy references

        Policy references are specific applications of policies. In this case
        we are only looking at row access and masking policies.

        This method will raise an exception if the Snowflake user doesn't
        Sets the policy_references instance variable. This method will raise
        an exception if the Snowflake user doesn't have permissions on the
        snowflake.account_usage.policy_references table.

        Returns:
            None
        """
        masking_policy_references = []
        row_access_policy_references = []
        with self.conn.cursor(DictCursor) as cur:
            statement = (
                "SELECT * FROM SNOWFLAKE.account_usage.policy_references WHERE"
                " policy_kind IN ('MASKING_POLICY', 'ROW_ACCESS_POLICY')"
            )
            for row in cur.execute(statement):
                masking_policy_reference = MaskingPolicyReference.from_row(row)
                if masking_policy_reference is not None:
                    masking_policy_references.append(masking_policy_reference)
                else:
                    row_access_policy_reference = RowAccessPolicyReference.from_row(row)
                    if row_access_policy_reference is not None:
                        row_access_policy_references.append(row_access_policy_reference)
        self.masking_policy_references = masking_policy_references
        self.row_access_policy_references = row_access_policy_references

    def fetch_roles(self):
        """Fetch all roles in the Snowflake account

        Returns:
            None
        """
        roles = []
        with self.conn.cursor(DictCursor) as cur:
            statement = "SHOW ROLES"
            for row in cur.execute(statement):
                roles.append(Role.from_row(row))
        self.roles = roles

    def fetch_schemas(self):
        """Fetch all schemas in the Snowflake account

        Fetches all schemas from each database separately to avoid the 10,000
        row limit of SHOW queries. These fetches are run concurrently with the
        help of the _fetch_schemas_for_single_db method.

        Returns:
            None
        """
        nested_lists = util.run_with_progress_bar(
            self._fetch_schemas_for_single_db, self.databases, self.max_workers
        )
        self.schemas = list(itertools.chain.from_iterable(nested_lists))

    def _fetch_schemas_for_single_db(self, db) -> list[Schema]:
        """Run the queries necessary to fetch all schemas from a db

        Returns:
            A list of all the schemas from the given database
        """
        schemas = []
        with self.conn.cursor(DictCursor) as cur:
            statement = f"SHOW SCHEMAS IN DATABASE {db.fqn()}"
            for row in cur.execute(statement):
                schemas.append(Schema.from_row(row))
        return schemas

    def fetch_entities(self):
        """Fetch all the tables and views from all schemas

        Fetches all tables and views from each schema separately to avoid the 10,000
        row limit of SHOW queries. These fetches are run concurrently with the
        help of the _fetch_entities_for_single_schema method.

        Returns:
            None

        """
        nested_lists = util.run_with_progress_bar(
            self._fetch_entities_for_single_schema, self.schemas, self.max_workers
        )
        self.entities = list(itertools.chain.from_iterable(nested_lists))

    def _fetch_entities_for_single_schema(self, schema) -> list[Entity]:
        """Run the queries necessary to fetch all tables and views from a schema

        Returns:
            A list of all the tables and views from the given schema
        """
        entities = []
        with self.conn.cursor(DictCursor) as cur:
            statement = f"SHOW OBJECTS IN SCHEMA {schema.fqn()}"
            for row in cur.execute(statement):
                entities.append(Entity.from_row(row))
        return entities

    def fetch_columns(self):
        """Fetch all the columns from all schemas

        Fetches all columns from each schema separately to avoid the 10,000
        row limit of SHOW queries. These fetches are run concurrently with the
        help of the _fetch_columns_for_single_schema method.

        Returns:
            None
        """

        nested_lists = util.run_with_progress_bar(
            self._fetch_columns_for_single_schema, self.schemas, self.max_workers
        )
        self.columns = list(itertools.chain.from_iterable(nested_lists))

    def _fetch_columns_for_single_schema(self, schema) -> list[Column]:
        """Run the queries necessary to fetch all columns from a schema

        Returns:
            A list of all the columns from the given schema
        """
        columns = []
        with self.conn.cursor(DictCursor) as cur:
            statement = f"SHOW COLUMNS IN SCHEMA {schema.fqn()}"
            for row in cur.execute(statement):
                columns.append(Column.from_row(row))
        return columns

    def fetch_role_grants(self):
        """Fetch all the grants of all the roles

        Fetches all grants of each role separately. These fetches are run
        concurrently with the help of the _fetch_role_grants_of_single_role
        method.

        Returns:
            None
        """

        nested_lists = util.run_with_progress_bar(
            self._fetch_role_grants_of_single_role, self.roles, self.max_workers
        )
        self.role_grants = list(itertools.chain.from_iterable(nested_lists))

    def _fetch_role_grants_of_single_role(self, role) -> list[RoleGrant]:
        """Run the queries necessary to fetch all grants of a role

        Returns:
            A list of all the grants of the given role
        """
        role_grants = []
        with self.conn.cursor(DictCursor) as cur:
            statement = f'SHOW GRANTS OF ROLE "{role.name}"'
            for row in cur.execute(statement):
                role_grants.append(RoleGrant.from_row(row))
        return role_grants

    def fetch_privilege_grants(self):
        """Fetch all the grants of all the privileges

        Fetches grants for every database, schema, table, and view. These
        fetches are run concurrently with the help of the
        _fetch_privilege_grants_to_single_object method.

        Returns:
            None
        """
        objects = [(x, "DATABASE") for x in self.databases]
        objects += [(x, "SCHEMA") for x in self.schemas]
        objects += [(x, "TABLE") for x in self.entities]

        nested_lists = util.run_with_progress_bar(
            self._fetch_privilege_grants_to_single_object, objects, self.max_workers
        )
        self.privilege_grants = list(itertools.chain.from_iterable(nested_lists))

    def _fetch_privilege_grants_to_single_object(
        self, object: tuple[Database | Schema | Entity, str]
    ) -> list[PrivilegeGrant]:
        """Build a PrivilegeGrant object for a single data asset

        Returns:
            A list of all the grants for the given object
        """
        object_name, object_type = object

        privilege_grants = []
        with self.conn.cursor(DictCursor) as cur:
            statement = f"SHOW GRANTS ON {object_type} {object_name.fqn()}"

            for row in cur.execute(statement):
                grant = PrivilegeGrant.from_row(row)
                if grant is not None:
                    privilege_grants.append(grant)
        return privilege_grants

    def fetch_future_grants(self):
        """Fetch all future grants

        Fetch the future grants in every database and schema, and set the
        future_grants attribute. Runs requests in parallel with the help
        of the _fetch_future_grants_from_single_asset method.

        Returns:
            None
        """
        nested_lists = util.run_with_progress_bar(
            self._fetch_future_grants_from_single_asset,
            [*self.databases, *self.schemas],
            self.max_workers,
        )
        self.future_grants = list(itertools.chain.from_iterable(nested_lists))

    def _fetch_future_grants_from_single_asset(self, asset) -> list[FutureGrant]:
        """Build a FutureGrant object for a single data asset

        Returns:
            A list of all the future grants for the given object
        """
        asset_type = "DATABASE" if asset.__class__ == Database else "SCHEMA"
        future_grants = []
        with self.conn.cursor(DictCursor) as cur:
            statement = f"SHOW FUTURE GRANTS IN {asset_type} {asset.fqn()}"
            for row in cur.execute(statement):
                future_grants.append(FutureGrant.from_row(row))
        return future_grants

    def fetch_environment(self) -> None:
        """Fetch metadata and populates instance variables

        This method should be run before any checks are run, as it provides
        the checks with the necessary metadata.

        Returns:
            None
        """

        print("\nChecking account level")
        self.check_is_enterprise_or_higher()
        print("\nChecking for network policies")
        print_query("SHOW NETWORK POLICIES")
        self.check_network_policy()
        print("\nFetching users")
        print_query(User.query)
        self.fetch_users()
        print("\nFetching roles")
        print_query(Role.query)
        self.fetch_roles()
        print("\nFetching databases")
        print_query(Database.query)
        self.fetch_databases()
        print("\nFetching login history")
        print_query(LoginHistory.query)
        self.fetch_login_history()

        # Fetch enterprise-only data
        if self.is_enterprise_or_higher:
            print("\n Fetching masking policies")
            print_query(MaskingPolicy.query)
            self.fetch_masking_policies()
            print("\nFetching row access policies")
            print_query(RowAccessPolicy.query)
            self.fetch_row_access_policies()
            print("\nAttempting to fetch masking and row access policy references")
            print_query(RowAccessPolicyReference.query)
            try:
                self.fetch_policy_references()
            except Exception as e:
                print(
                    "~~~ Unable to fetch policy references. This is likely due to"
                    " insufficient privileges.\n~~~ Feel free to try again later with a"
                    " user that has access to SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES"
                )
                print(e)
            print("\nAttempting to fetch access history")
            print_query(AccessHistory.query)
            try:
                self.fetch_access_history()
            except Exception as e:
                print(
                    "~~~ Unable to fetch access history. This is likely due to"
                    " insufficient privileges.\n~~~ Feel free to try again later with a"
                    " user that has access to SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY"
                )
                print(e)

        print("\nFetching schemas for each database")
        print_query(Schema.query)
        self.fetch_schemas()
        print("\nFetching tables and views for each schema")
        print_query(Entity.query)
        self.fetch_entities()
        print("\nFetching columns for each schema")
        print_query(Column.query)
        self.fetch_columns()
        print("\nFetching grants of each role")
        print_query(RoleGrant.query)
        self.fetch_role_grants()
        print("\nFetching grants to each database, schema, and table/view")
        print_query(PrivilegeGrant.query)
        self.fetch_privilege_grants()
        print("\nFetching future grants grants in each database and schema")
        print_query(FutureGrant.query)
        self.fetch_future_grants()
        print("\nSuccessfully fetched environment details 🎉🎉")

    @property
    def html(self) -> str:
        """Returns the HTML used to populate this check in the scorecard

        As part of this, we will also sort the checks so that they come back in
        order of importance.

        Returns:
            str: The HTML used for the scorecard
        """
        jinja_env = Environment(loader=PackageLoader("jetty_scorecard"))
        template = jinja_env.get_template("base.html.jinja")

        self.checks.sort(key=lambda x: x.title)
        self.checks.sort(key=checks.score_map)

        return template.render(
            grade=self.grade,
            grade_color=util.GRADE_COLORS[self.grade],
            pass_count=self.num_pass_checks,
            warn_count=self.num_warn_checks,
            idea_count=self.num_info_checks,
            insight_count=self.num_insight_checks,
            fail_count=self.num_fail_checks,
            unknown_count=self.num_unknown_checks,
            checks=[check.html for check in self.checks],
        )


class DataAsset:
    """Represents Data Assets (like databases and schemas)"""

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} fqn: '{self.fqn()}' owner: '{self.owner}'>"


class Database(DataAsset, Queryable):
    """Database metadata from Snowflake

    Attributes:
      name: name of the database (unquoted)
      owner: owner of the database (cleaned, not quoted)
      query: class attribute of the query used to generate the metadata;
    """

    name: str
    owner: str
    query: str = "SHOW DATABASES;"

    def __init__(self, name: str, owner: str) -> None:
        """New Database instance

        Args:
            name: name of the database (as returned from the db)
            owner: owner of the database (cleaned, not quoted)
        """
        self.name = name
        self.owner = owner

    def fqn(self) -> str:
        """
        Returns:
            Fully qualified name of the database
        """
        return util.fqn(self.name)

    # Transform a row from the SHOW DATABASES query into a database object
    def from_row(row: dict) -> Database:
        """New Database instance from a query result row

        Args:
            row: a row from the SHOW DATABASES query

        Returns:
            New Database instance
        """
        return Database(row["name"], util.clean_up_identifier_name(row["owner"]))


class Schema(DataAsset, Queryable):
    """Schema metadata from Snowflake

    Attributes:
      name: name of the schema (unquoted)
      database: name of the database (unquoted)
      owner: owner of the schema (cleaned, not quoted)
      managed_access: whether the schema uses managed access policies
      query: class attribute of the query used to generate the metadata;
    """

    name: str
    database: str
    owner: str
    managed_access: bool
    query: str = "SHOW SCHEMAS IN DATABASE <database name>;"

    def __init__(self, name: str, database: str, owner: str, managed_access: bool):
        """New Schema instance

        Args:
            name: name of the schema (as returned from the db)
            database: name of the database (as returned from the db)
            owner: owner of the schema (cleaned, not quoted)
            managed_access: whether the schema uses managed access policies
        """
        self.name = name
        self.database = database
        self.owner = owner
        self.managed_access = managed_access

    def fqn(self) -> str:
        """
        Returns:
            Fully qualified name of the schema (quoted)
        """
        return util.fqn(self.database, self.name)

    @classmethod
    def from_row(cls, row: tuple) -> Schema:
        """New Schema instance from a query result row

        Args:
            row: a row from the SHOW SCHEMAS query

        Returns:
            New Schema instance
        """
        return cls(
            row["name"],
            row["database_name"],
            util.clean_up_identifier_name(row["owner"]),
            "MANAGED ACCESS" in row["options"],
        )


class Entity(DataAsset, Queryable):
    """Entity metadata from Snowflake

    Attributes:
      name: name of the entity (unquoted)
      database: name of the database (unquoted)
      schema: name of the schema (unquoted)
      owner: owner of the entity (cleaned, not quoted)
      entity_type: type of the entity
      query: class attribute of the query used to generate the metadata
    """

    name: str
    owner: str
    database: str
    schema: str
    entity_type: str
    query: str = "SHOW OBJECTS IN SCHEMA <schema name>;"

    def __init__(
        self, name: str, database: str, schema: str, owner: str, entity_type: str
    ):
        """Initialize a new Entity

        Args:
            name: name of the entity (as returned from the db)
            database: name of the database (as returned from the db)
            schema: name of the schema (as returned from the db)
            owner: owner of the entity (cleaned, not quoted)
            entity_type: type of the entity (e.g., Table or View)
        """
        self.name = name
        self.database = database
        self.schema = schema
        self.owner = owner
        self.entity_type = entity_type

    def fqn(self) -> str:
        """
        Returns:
            Fully qualified name of the entity
        """
        return util.fqn(self.database, self.schema, self.name)

    @classmethod
    def from_row(cls, row: tuple) -> Entity:
        """New Entity instance from a query result row

        Args:
            row: a row from the SHOW OBJECTS IN SCHEMA <schema name> query

        Returns:
            New Entity instance
        """
        return cls(
            row["name"],
            row["database_name"],
            row["schema_name"],
            util.clean_up_identifier_name(row["owner"]),
            row["kind"],
        )


class Column(DataAsset, Queryable):
    """Column metadata from Snowflake

    Attributes:
      name: name of the column (unquoted)
      database: name of the database (unquoted)
      schema: name of the schema (unquoted)
      table: name of the table (unquoted)
      query: class attribute of the query used to generate the metadata
    """

    name: str
    database: str
    schema: str
    table: str
    query: str = "SHOW COLUMNS IN SCHEMA <schema name>;"

    def __init__(self, name: str, database: str, schema: str, table: str):
        """Initialize a new Column

        Args:
            name: name of the column (as returned from the db)
            database: name of the database (as returned from the db)
            schema: name of the schema (as returned from the db)
            table: name of the table (as returned from the db)
        """
        self.name = name
        self.database = database
        self.schema = schema
        self.table = table

    def fqn(self) -> str:
        """
        Returns:
            Fully qualified, quoted name of the column
        """
        return util.fqn(self.database, self.schema, self.table, self.name)

    @classmethod
    def from_row(cls, row: tuple) -> Column:
        """New Column instance from a query result row

        Args:
            row: a row from the SHOW COLUMNS query

        Returns:
            New Column instance
        """
        return cls(
            row["column_name"],
            row["database_name"],
            row["schema_name"],
            row["table_name"],
        )


class User(Queryable):
    """User metadata from Snowflake

    Attributes:
      name: name of the user (cleaned, not quoted)
      disabled: bool specifying whether the user is disabled
      owner: owner of the user (cleaned, not quoted)
      last_successful_login: the datetime of the last time the user logged in
      has_password: bool specifying whether the user has a password
      query: class attribute of the query used to generate the metadata
    """

    name: str
    disabled: bool
    owner: str
    last_successful_login: datetime
    has_password: bool
    query: str = "SHOW USERS;"

    def __init__(
        self,
        name: str,
        disabled: bool,
        owner: str,
        last_successful_login: datetime,
        has_password: bool,
    ):
        """
        Args:
            name: name of the user (cleaned, not quoted)
            disabled: bool specifying whether the user is disabled
            owner: owner of the user (cleaned, not quoted)
            last_successful_login: the datetime of the last time the user
                logged in
            has_password: bool specifying whether the user has a password
        """
        self.name = name
        self.disabled = disabled
        self.owner = owner
        self.last_successful_login = last_successful_login
        self.has_password = has_password

    def __repr__(self) -> str:
        return (
            f"<User {self.name} disabled: {self.disabled} owner:"
            f" {self.owner} last_login: {self.last_successful_login} has_password:"
            f" {self.has_password}>"
        )

    @classmethod
    def from_row(cls, row: tuple) -> User:
        """New User instance from a query result row

        Args:
            row: a row from the SHOW USERS query

        Returns:
            New User instance
        """
        return cls(
            util.clean_up_identifier_name(row["name"]),
            row["disabled"] == "true",
            util.clean_up_identifier_name(row["owner"]),
            row["last_success_login"],
            row["has_password"] == "true",
        )


class Role(Queryable):
    """Role metadata from Snowflake

    Attributes:
      name: name of the role (cleaned, not quoted)
      owner: owner of the role (cleaned, not quoted)
      query: class attribute of the query used to generate the metadata
    """

    name: str
    owner: str
    query: str = "SHOW ROLES;"

    def __init__(self, name: str, owner: str) -> None:
        """
        Args:
            name: name of the role (cleaned, not quoted)
            owner: owner of the role (cleaned, not quoted)
        """
        self.name = name
        self.owner = owner

    def __repr__(self) -> str:
        return self.name

    @classmethod
    def from_row(cls, row: tuple) -> Role:
        """New Role instance from a query result row

        Args:
            row: a row from the SHOW ROLES query

        Returns:
            New Role instance
        """
        return cls(
            util.clean_up_identifier_name(row["name"]),
            util.clean_up_identifier_name(row["owner"]),
        )


class RoleGrant(Queryable):
    """Role grant metadata from Snowflake

    Attributes:
      role: name of the role (cleaned, not quoted)
      grantee: name of the grantee (cleaned, not quoted)
      grantee_type: type of the grantee
      granted_by: user who granted the role (cleaned, not quoted)
      query: class attribute of the query used to generate the metadata
    """

    role: str
    grantee: str
    grantee_type: str
    granted_by: str
    query: str = "SHOW GRANTS OF ROLE <role name>;"

    def __init__(
        self, role: str, grantee: str, grantee_type: str, granted_by: str
    ) -> None:
        """
        Args:
            role: name of the role (cleaned, not quoted)
            grantee: name of the grantee (cleaned, not quoted)
            grantee_type: type of the grantee (as returned form the db)
            granted_by: user who granted the role (cleaned, not quoted)
        """
        self.role = role
        self.grantee = grantee
        self.grantee_type = grantee_type
        self.granted_by = granted_by

    def __repr__(self) -> str:
        return (
            f"<RoleGrant {self.role} TO {self.grantee_type} {self.grantee} granted_by:"
            f" {self.granted_by}>"
        )

    @classmethod
    def from_row(cls, row: tuple) -> RoleGrant:
        """New RoleGrant instance from a query result row

        Args:
            row: a row from the SHOW GRANTS OF ROLE query

        Returns:
            New RoleGrant instance
        """
        return cls(
            util.clean_up_identifier_name(row["role"]),
            util.clean_up_identifier_name(row["grantee_name"]),
            row["granted_to"],
            util.clean_up_identifier_name(row["granted_by"]),
        )


class PrivilegeGrant(Queryable):
    """Privilege grant metadata from Snowflake

    Attributes:
        asset: fqn of the asset the privilege is granted (quoted)
        asset_type: type of the asset the privilege is granted
        grantee: name of the grantee (cleaned, not quoted)
        grant_option: bool of whether the grantee can grant this
          privilege to other roles
        privilege: name of the privilege granted
        granted_by: user who granted the privilege (cleaned, not quoted)
        query: class attribute of the query used to generate the metadata
    """

    asset: str
    asset_type: str
    grantee: str
    grant_option: bool
    privilege: str
    granted_by: str
    query: str = "SHOW GRANTS ON <object type> <object name>;"

    def __init__(
        self,
        asset: str,
        asset_type: str,
        grantee: str,
        grant_option: bool,
        privilege: str,
        granted_by: str,
    ):
        """
        Args:
            asset: fqn of the asset the privilege is granted (quoted)
            asset_type: type of the asset the privilege is granted
            grantee: name of the grantee (cleaned, not quoted)
            grant_option: bool of whether the grantee can grant this
            privilege: name of the privilege granted
            granted_by: user who granted the privilege (cleaned, not quoted)
        """
        self.asset = asset
        self.asset_type = asset_type
        self.grantee = grantee
        self.grant_option = grant_option
        self.privilege = privilege
        self.granted_by = granted_by

    def __repr__(self) -> str:
        return (
            "<PrivilegeGrant"
            f" asset:{self.asset} asset_type:{self.asset_type} grantee:{self.grantee} grant_option:{self.grant_option} privilege:{self.privilege} granted_by:{self.granted_by}>"
        )

    @classmethod
    def from_row(cls, row: tuple) -> PrivilegeGrant | None:
        """New PrivilegeGrant instance from a query result row

        Args:
            row: a row from the SHOW GRANTS ON <object type> <object name> query

        Returns:
            New PrivilegeGrant instance
        """
        if row["granted_on"] == "ROLE":
            return
        # FUTURE: Modify this to also work with database roles
        if row["granted_to"] != "ROLE":
            return
        else:
            name = util.add_missing_quotes_to_fqn(row["name"])
            return cls(
                name,
                row["granted_on"],
                util.clean_up_identifier_name(row["grantee_name"]),
                row["grant_option"],
                row["privilege"],
                util.clean_up_identifier_name(row["granted_by"]),
            )


class FutureGrant(Queryable):
    """Future grant metadata from Snowflake

    Attributes:
        target: name of the target (unquoted, as returned from db)
        asset_type: type of the target
        grantee: name of the grantee (cleaned, not quoted)
        grant_option: bool of whether the the grant option will be granted
          as well
        privilege: name of the privilege to be granted
        query: class attribute of the query used to generate the metadata
    """

    target: str
    asset_type: str
    grantee: str
    grant_option: bool
    privilege: str
    query: str = "SHOW FUTURE GRANTS IN <object type> <object name>;"

    def __init__(
        self,
        target: str,
        asset_type: str,
        grantee: str,
        grant_option: bool,
        privilege: str,
    ):
        """
        Args:
            target: name of the target (unquoted, as returned from db)
            asset_type: type of the target
            grantee: name of the grantee (cleaned, not quoted)
            grant_option: bool of whether the the grant option will be granted
            privilege: name of the privilege to be granted
        """
        self.target = target
        self.asset_type = asset_type
        self.grantee = grantee
        self.grant_option = grant_option
        self.privilege = privilege

    def __repr__(self) -> str:
        return (
            "<FutureGrant"
            f" {self.target} grantee:{self.grantee} grant_option:{self.grant_option} privilege:{self.privilege}>"
        )

    @classmethod
    def from_row(cls, row: tuple) -> FutureGrant:
        """New FutureGrant instance from a query result row

        Args:
            row: a row from the SHOW FUTURE GRANTS IN <object type> <object name> query

        Returns:
            New FutureGrant instance
        """
        return cls(
            row["name"],
            row["grant_on"],
            util.clean_up_identifier_name(row["grantee_name"]),
            row["grant_option"],
            row["privilege"],
        )

    @property
    def set_on(self) -> str:
        """
        Returns:
            FQN of the parent asset the future grant is set on.
        """
        name_part = self.target.split(".<")[0]
        return util.add_missing_quotes_to_fqn(name_part)


class MaskingPolicy(Queryable):
    """Masking policy metadata from Snowflake

    Attributes:
        name: name of the masking policy (unquoted)
        database: name of the database (unquoted)
        schema: name of the schema (unquoted)
        owner: name of the owner (cleaned, not quoted)
        query: class attribute of the query used to generate the metadata
    """

    name: str
    database: str
    schema: str
    owner: str
    query: str = "SHOW MASKING POLICIES;"

    def __init__(self, name: str, database: str, schema: str, owner: str) -> None:
        """
        Args:
            name: name of the masking policy (unquoted)
            database: name of the database (unquoted)
            schema: name of the schema (unquoted)
            owner: name of the owner (cleaned, not quoted)
        """
        self.name = name
        self.database = database
        self.schema = schema
        self.owner = owner

    def fqn(self) -> str:
        """
        Returns:
            FQN of the masking policy
        """
        return util.fqn(self.database, self.schema, self.name)

    def __repr__(self) -> str:
        return f"<MaskingPolicy {self.fqn()} owner:{self.owner}>"

    # for the query: SHOW MASKING POLICIES
    @classmethod
    def from_row(cls, row: tuple) -> MaskingPolicy:
        """New MaskingPolicy instance from a query result row

        Args:
            row: a row from the SHOW MASKING POLICIES query

        Returns:
            New MaskingPolicy instance
        """
        return cls(
            row["name"],
            row["database_name"],
            row["schema_name"],
            util.clean_up_identifier_name(row["owner"]),
        )


class RowAccessPolicy(Queryable):
    """Row access policy metadata from Snowflake

    Attributes:
        name: name of the row access policy (unquoted)
        database: name of the database (unquoted)
        schema: name of the schema (unquoted)
        owner: name of the owner (cleaned, not quoted)
        query: class attribute of the query used to generate the metadata
    """

    name: str
    database: str
    schema: str
    owner: str
    query: str = "SHOW ROW ACCESS POLICIES;"

    def __init__(self, name: str, database: str, schema: str, owner: str) -> None:
        """
        Args:
            name: name of the row access policy (unquoted)
            database: name of the database (unquoted)
            schema: name of the schema (unquoted)
            owner: name of the owner (cleaned, not quoted)
        """
        self.name = name
        self.database = database
        self.schema = schema
        self.owner = owner

    def fqn(self) -> str:
        """
        Returns:
            FQN of the row access policy.
        """
        return util.fqn(self.database, self.schema, self.name)

    def __repr__(self) -> str:
        return f"<RowAccessPolicy {self.fqn()} owner:{self.owner}>"

    @classmethod
    def from_row(cls, row: tuple) -> RowAccessPolicy:
        """New RowAccessPolicy instance from a query result row

        Args:
            row: a row from the SHOW ROW ACCESS POLICIES query

        Returns:
            New RowAccessPolicy instance
        """
        return cls(
            row["name"],
            row["database_name"],
            row["schema_name"],
            util.clean_up_identifier_name(row["owner"]),
        )


class LoginHistory(Queryable):
    """Login history metadata from Snowflake

    Attributes:
        user: name of the user logging in (cleaned, not quoted)
        first_authentication_factor: name of the first authentication factor
          (as returned by database)
        second_authentication_factor: name of the second authentication factor
        success: boolean indicating whether the login was successful
        query: class attribute of the query used to generate the metadata
    """

    user: str
    first_authentication_factor: str
    second_authentication_factor: str
    success: bool
    query: str = "SELECT * FROM table(snowflake.information_schema.login_history());"

    def __init__(
        self,
        username: str,
        first_authentication_factor: str,
        second_authentication_factor: str,
        success: bool,
    ):
        """
        Args:
            user: name of the user logging in (cleaned, not quoted)
            first_authentication_factor: name of the first authentication factor (unquoted)
            second_authentication_factor: name of the second authentication factor (unquoted)
            success: boolean indicating whether the login was successful
        """
        self.user = username
        self.first_authentication_factor = first_authentication_factor
        self.second_authentication_factor = second_authentication_factor
        self.success = success

    def __repr__(self) -> str:
        return (
            "<LoginHistory"
            f" {self.user} {self.first_authentication_factor} mfa:{self.second_authentication_factor}>"
        )

    # for the query: SELECT * FROM table(snowflake.information_schema.login_history())
    @classmethod
    def from_row(cls, row: tuple) -> LoginHistory:
        """New LoginHistory instance from a query result row

        Args:
            row: a row from the SHOW LOGIN HISTORY query

        Returns:
            New LoginHistory instance
        """
        return cls(
            util.clean_up_identifier_name(row["USER_NAME"]),
            row["FIRST_AUTHENTICATION_FACTOR"],
            row["SECOND_AUTHENTICATION_FACTOR"],
            row["IS_SUCCESS"] == "YES",
        )


class AccessHistory(Queryable):
    """Table and Column access history from the last 90 days

    Attributes:
        tables: dataframe of user, asset, and usage count
        columns: dataframe of users, asset, and usage count
    """

    tables: pd.DataFrame
    columns: pd.DataFrame
    query: str = (
        "SELECT user_name, direct_objects_accessed FROM"
        " snowflake.account_usage.access_history WHERE query_start_time >"
        " DATEADD('DAY', -90, CURRENT_TIMESTAMP());"
    )

    def __init__(self, tables: pd.DataFrame, columns: pd.DataFrame):
        """
        Args:
            tables: dataframe of user, asset, and usage count
            columns: dataframe of users, asset, and usage count
        """
        self.tables = tables
        self.columns = columns

    def __repr__(self) -> str:
        return f"<AccessHistory>"

    # for the query: SELECT user_name, direct_objects_accessed FROM snowflake.account_usage.access_history WHERE query_start_time > DATEADD('DAY', -90, CURRENT_TIMESTAMP());
    @classmethod
    def from_rows(cls, rows: list[tuple]) -> AccessHistory:
        """New AccessHistory instance from a query result rows

        Args:
            rows: a list of rows from the SELECT user_name, direct_objects_accessed FROM snowflake.account_usage.access_history WHERE query_start_time > DATEADD('DAY', -90, CURRENT_TIMESTAMP()) query

        Returns:
            New AccessHistory instance
        """
        columns = {}
        tables = {}
        for row in rows:
            bool_tables = {}
            bool_columns = {}
            user = util.clean_up_identifier_name(row["USER_NAME"])
            res_obj = json.loads(row["DIRECT_OBJECTS_ACCESSED"])
            for ref in res_obj:
                if ref.get("objectDomain") is None or ref["objectDomain"] not in (
                    "Table",
                    "View",
                ):
                    continue
                table_name = util.quote_fqn(ref["objectName"])
                bool_tables[(user, table_name)] = True

                if ref.get("columns") is None:
                    continue
                for column in ref["columns"]:
                    column_name = f"{table_name}.{util.quote_fqn(column['columnName'])}"
                    bool_columns[(user, column_name)] = True

            for k in bool_tables:
                tables[k] = tables.get(k, 0) + 1
            for k in bool_columns:
                columns[k] = columns.get(k, 0) + 1

        columns_df = pd.DataFrame.from_records(
            [(*k, v) for k, v in columns.items()],
            columns=["user", "object", "usage_count"],
        )
        tables_df = pd.DataFrame.from_records(
            [(*k, v) for k, v in tables.items()],
            columns=["user", "object", "usage_count"],
        )

        return cls(tables_df, columns_df)


class MaskingPolicyReference(Queryable):
    """
    Masking policy reference data

    Attributes:
        name: policy name (unquoted)
        database: database name (unquoted)
        schema: schema name (unquoted)
        policy_id: int
        target_fqn: fully qualified name of the target data (a column)
          (quoted)
        tag_fqn: fully qualified name of the tag that this policy is applied
         through (quoted)
        status: status of the masking policy, as returned form the database
        query: query to get the masking policy reference data
    """

    name: str
    database: str
    schema: str
    policy_id: int
    target_fqn: str
    tag_fqn: str | None
    status: str
    query: str = (
        "SELECT * FROM SNOWFLAKE.account_usage.policy_references WHERE policy_kind IN"
        " ('MASKING_POLICY', 'ROW_ACCESS_POLICY')"
    )

    def __init__(
        self,
        name: str,
        database: str,
        schema: str,
        policy_id: int,
        target_fqn: str,
        tag_fqn: str | None,
        status: str,
    ):
        """
        Args:
            name: policy name (unquoted)
            database: database name (unquoted)
            schema: schema name (unquoted)
            policy_id: int
            target_fqn: fully qualified name of the target data (a column)
              (quoted)
            tag_fqn: fully qualified name of the tag that this policy is applied
              through (quoted)
            status: status of the masking policy, as returned form the database
        """
        self.name = name
        self.database = database
        self.schema = schema
        self.policy_id = policy_id
        self.target_fqn = target_fqn
        self.tag_fqn = tag_fqn
        self.status = status

    def fqn(self) -> str:
        """
        Returns:
            fully qualified name of the masking policy
        """
        return util.fqn(self.database, self.schema, self.name)

    def __repr__(self) -> str:
        return (
            "<MaskingPolicyReference"
            f" {self.fqn()} id:{self.policy_id} target:{self.target_fqn} tag:{self.tag_fqn} status:{self.status}>"
        )

    @classmethod
    def from_row(cls, row: tuple) -> MaskingPolicyReference | None:
        """New MaskingPolicyReference instance from a query result row

        Args:
            row: a row from the SELECT * FROM SNOWFLAKE.account_usage.policy_references WHERE policy_kind IN ('MASKING_POLICY', 'ROW_ACCESS_POLICY') query

        Returns:
            New MaskingPolicyReference instance
        """
        if row["POLICY_KIND"] == "MASKING_POLICY":
            target_fqn = util.fqn(
                row["REF_DATABASE_NAME"],
                row["REF_SCHEMA_NAME"],
                row["REF_ENTITY_NAME"],
                row["REF_COLUMN_NAME"],
            )
            tag_fqn = (
                None
                if row["TAG_DATABASE"] is None
                else util.fqn(row["TAG_DATABASE"], row["TAG_SCHEMA"], row["TAG_NAME"])
            )
            return cls(
                row["POLICY_NAME"],
                row["POLICY_DB"],
                row["POLICY_SCHEMA"],
                row["POLICY_ID"],
                target_fqn,
                tag_fqn,
                row["POLICY_STATUS"],
            )


class RowAccessPolicyReference(Queryable):
    """
    RowAccessPolicyReference policy reference metadata

    Attributes:
        name: policy name (unquoted)
        database: database name (unquoted)
        schema: schema name (unquoted)
        policy_id: int
        target_fqn: fully qualified name of the target data (a table)
          (quoted)
        tag_fqn: fully qualified name of the tag that this policy is applied
         through (quoted)
        status: status of the masking policy, as returned form the database
        query: query to get the masking policy reference data
    """

    name: str
    database: str
    schema: str
    policy_id: int
    target_fqn: str
    tag_fqn: str | None
    status: str
    query: str = (
        "SELECT * FROM SNOWFLAKE.account_usage.policy_references WHERE policy_kind IN"
        " ('MASKING_POLICY', 'ROW_ACCESS_POLICY')"
    )

    def __init__(
        self,
        name: str,
        database: str,
        schema: str,
        policy_id: int,
        target_fqn: str,
        tag_fqn: str | None,
        status: str,
    ):
        """
        Args:
            name: policy name (unquoted)
            database: database name (unquoted)
            schema: schema name (unquoted)
            policy_id: int
            target_fqn: fully qualified name of the target data (a table)
              (quoted)
            tag_fqn: fully qualified name of the tag that this policy is applied
              through (quoted)
            status: status of the masking policy, as returned form the database
        """
        self.name = name
        self.database = database
        self.schema = schema
        self.policy_id = policy_id
        self.target_fqn = target_fqn
        self.tag_fqn = tag_fqn
        self.status = status

    def fqn(self) -> str:
        """
        Returns:
            fully qualified name of the row access policy
        """
        return util.fqn(self.database, self.schema, self.name)

    def __repr__(self) -> str:
        return (
            "<RowAccessPolicyReference"
            f" {self.fqn()} id:{self.policy_id} target:{self.target_fqn} tag:{self.tag_fqn} status:{self.status}>"
        )

    # for the query: SELECT * FROM SNOWFLAKE.account_usage.policy_references WHERE policy_kind IN ('MASKING_POLICY', 'ROW_ACCESS_POLICY')
    @classmethod
    def from_row(cls, row: tuple) -> RowAccessPolicyReference | None:
        """New RowAccessPolicyReference instance from a query result row

        Args:
            row: a row from the SELECT * FROM SNOWFLAKE.account_usage.policy_references WHERE policy_kind IN ('MASKING_POLICY', 'ROW_ACCESS_POLICY') query

        Returns:
            New RowAccessPolicyReference instance
        """
        if row["POLICY_KIND"] == "ROW_ACCESS_POLICY":
            target_fqn = util.fqn(
                row["REF_DATABASE_NAME"], row["REF_SCHEMA_NAME"], row["REF_ENTITY_NAME"]
            )
            tag_fqn = (
                None
                if row["TAG_DATABASE"] is None
                else util.fqn(row["TAG_DATABASE"], row["TAG_SCHEMA"], row["TAG_NAME"])
            )
            return cls(
                row["POLICY_NAME"],
                row["POLICY_DB"],
                row["POLICY_SCHEMA"],
                row["POLICY_ID"],
                target_fqn,
                tag_fqn,
                row["POLICY_STATUS"],
            )


class RoleGrantNodeType(Enum):
    ROLE = auto()
    USER = auto()


def print_query(query: str) -> None:
    """
    Prints a query to the console

    Args:
        query: query to print

    Returns:
        None
    """
    print(f"    {TextFormat.ITALIC}{TextFormat.LIGHT_GRAY}{query}{TextFormat.RESET}")
