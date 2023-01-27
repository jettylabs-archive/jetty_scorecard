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


def _runner(chk: Check, _: SnowflakeEnvironment):
    """Runner function for the test check"""
    chk.score = random()
    chk.details = f"""
Hello. I'm glad you could give this a shot.
<p> score: {chk.score} </p>
<ul>
    <li>Here's one idea</li>
    <li>Here's another idea</li>
    <li>Here's a third idea</li>
<ul>
"""
    time.sleep(random() / 3)
