"""Checks to be run against a SnowflakeEnvironment"""
from __future__ import annotations

from enum import Enum
from typing import Callable
import uuid
from jinja2 import Environment, PackageLoader
from jetty_scorecard import env


class CheckStatus(Enum):
    """The available statuses for checks

    Attributes:
        INFO: The check provides useful information (but didn't provide a
          score)
        FAIL: The check has failed
        WARN: The check generated a warning
        PASS: The check has passed
        INSIGHT: The check provides insights (but didn't provide a score)
        UNKNOWN: The check didn't run, or didn't provide status for some
          reason
    """

    INFO = 1
    FAIL = 2
    PASS = 3
    INSIGHT = 4
    UNKNOWN = 5
    WARN = 6


class Check:
    """A class that represents a check.

    Checks are used to provide feedback to users based on their configurations.
    They are run against a Snowflake environment.

    Attributes:
        title: The title of the check
        subtitle: The subtitle of the check
        description: The description of the check
        links: A list of tuples of (url, description). Provides useful links
          that are relevant to the check
        objects: A list of Queryable objects that the check uses. This is used
          to show the queries that were used to run the checks
        score: The score of the check. Generated after the check has run.
        details: The details provided after the check runs. These details
          should include information that will help users understand in improve
          their Snowflake configuration
        runner: A function that actually runs the check. This is where the
          check-specific logic lives
    """

    title: str
    subtitle: str
    description: str
    links: list[tuple[str, str]]
    objects: list[type]
    score: float | None
    details: str | None
    runner: Callable[[env.SnowflakeEnvironment], None]

    def __init__(
        self,
        title: str,
        subtitle: str,
        description: str,
        links: list[tuple[str, str]],
        objects: list[type],
        runner: Callable[[Check, env.SnowflakeEnvironment], None],
    ) -> None:
        """
        Args:
            title: The title of the check
            subtitle: The subtitle of the check
            description: The description of the check
            links: A list of tuples of (url, description). Provides useful links
              that are relevant to the check
            objects: A list of Queryable objects that the check uses.
              This is used to show the queries that were used to run the
              checks
            runner: A function that actually runs the check. This is
              where the check-specific logic lives"""
        self.title = title
        self.subtitle = subtitle
        self.description = description
        self.links = links
        self.score = None
        self.details = None
        self.objects = objects
        self.runner = runner

    def __repr__(self) -> str:
        return f"<Check {self.title}>"

    def run(self, environment: env.SnowflakeEnvironment) -> None:
        """Runs the check

        Args:
            environment: The Snowflake environment to run the check against
        """
        self.runner(self, environment)

    @property
    def queries(self) -> list[str]:
        """Returns a list of the queries that were used to run the check

        Returns:
            list[str]: A list of the queries that were used to run the check
        """
        if self.objects is None:
            return []
        return [o.query for o in self.objects if issubclass(o, env.Queryable)]

    @property
    def status(self):
        """Returns the status of the check

        Returns:
            The status of the check
        """
        return score_to_status(self.score)

    @property
    def html(self) -> str:
        """Returns the HTML used to populate this check in the scorecard

        Returns:
            str: The HTML used for the scorecard
        """
        jinja_env = Environment(loader=PackageLoader("jetty_scorecard"))
        template = jinja_env.get_template("check.html.jinja")
        return template.render(
            id=str(uuid.uuid4()),
            status=self.status.value,
            title=self.title,
            subtitle=self.subtitle,
            description=self.description,
            links=self.links,
            queries=self.queries,
            details=self.details,
        )


def score_to_status(score: float):
    """Converts a score to a status

    Args:
        score: The score to convert to a status

    Returns:
        CheckStatus: The status of the check (based on the score)
    """
    if score is None:
        return CheckStatus.UNKNOWN
    elif score == -2:
        return CheckStatus.INSIGHT
    elif score == -1:
        return CheckStatus.INFO
    elif score < 0.5:
        return CheckStatus.FAIL
    elif score < 0.9:
        return CheckStatus.WARN
    else:
        return CheckStatus.PASS


def score_map(check: Check) -> float:
    """Maps a Check to a float to be used in ordering the checks

    We'd like to order checks by order of importance - first fail, then
    warn, then pass, then info, insight, and unknown.

    This function is intended to be used with the sort method on env.checks

    Args:
        check: The Check to map

    Returns:
        float: The mapped sort value
    """

    if check.status == CheckStatus.UNKNOWN:
        return 100
    elif check.status == CheckStatus.INSIGHT:
        return 99
    elif check.status == CheckStatus.INFO:
        return 98
    else:
        return check.score
