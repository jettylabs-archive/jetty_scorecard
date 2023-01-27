from __future__ import annotations

from jetty_scorecard.checks import Check, CheckStatus
from jetty_scorecard.env import Database, SnowflakeEnvironment
import time
from random import random


def create() -> Check:
    """Create a test check.

    Returns:
        Check: The created check.
    """
    return Check(
        "Test Check",
        "Subtitle",
        "Here's my beautiful description",
        [
            ("http://www.google.com", "Google Homepage"),
            ("https://docs.get-jetty.com", "Jetty Docs"),
        ],
        [Database],
        _runner,
    )


def _runner(_: SnowflakeEnvironment) -> tuple[float, str]:
    """Runner function for the test check"""
    score = random()
    details = f"""
Hello. I'm glad you could give this a shot.
<p> score: {score} </p>
<ul>
    <li>Here's one idea</li>
    <li>Here's another idea</li>
    <li>Here's a third idea</li>
<ul>
"""
    time.sleep(random() / 3)
    return (score, details)
