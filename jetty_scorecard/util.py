"""Utility functions for building a scorecard"""

from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

"""The background colors for the grade component of the scorecard"""
GRADE_COLORS = {
    "A": "#4C8000da",
    "A-": "#709500da",
    "B+": "#99AA00da",
    "B": "#D5B800da",
    "B-": "#EAA700da",
    "C+": "#FF9101da",
    "C": "#FF8000da",
    "C-": "#FF6E00da",
    "D+": "#FF5B00da",
    "D": "#FF4700da",
    "D-": "#FF3100da",
    "F": "#FF1B00da",
    "?": "#808080da",
}

"""Number of workers to use when running queries"""
DEFAULT_MAX_WORKERS = 50


def percentage_to_grade(percentage, bottom=0.25, top=1) -> str:
    """Convert a percentage to a grade
    Args:
        bottom (int): the percentage where the bottom grade (F) starts
        top (int): the top of the scale (default 1)
        percentage (int): the percentage to convert into a grade

        Returns:
            str: the grade corresponding to the percentage"""
    grades = {
        1: "A+",
        2: "A",
        3: "A-",
        4: "B+",
        5: "B",
        6: "B-",
        7: "C+",
        8: "C",
        9: "C-",
        10: "D+",
        11: "D",
        12: "D-",
        13: "F",
    }
    if percentage >= top:
        return "A+"
    if percentage < bottom:
        return "F"
    span = top - bottom
    adjusted_score = percentage - bottom
    grade_level = ceil(12 - adjusted_score / span * 11)
    return grades[grade_level]


def clean_up_identifier_name(name: str) -> str:
    """Clean an identifier so that it can be quoted

    Some Snowflake queries return role names (and maybe user names) quoted,
    but others return them without quotes. This removes quotes from all of them.
    The result can be quoted in queries.

    Note- this doesn't address all edge cases. For example, if a role name
    actually starts with a double quote, I don't think it will be handled properly.

    Args:
        name (str): the identifier name

    Returns:
        str: the identifier with proper casing, ready to be quoted
    """
    if name.startswith('"'):
        return strip_one_char(name, '"')
    else:
        return name


def clean_up_asset_name(name: str) -> str:
    """Clean an asset name so that it can be quoted

    This manages quoted and strangely cased names

    Args:
        name (str): the asset name

    Returns:
        str: the name, ready to be quoted
    """
    if name.startswith('"'):
        # This will double the quotes. Then, when it is used to create an
        # fqn, it will end up with the 3 quotes needed
        return f'"{name}"'
    else:
        return name


def strip_one_char(s: str, c: str):
    """Strip a single character from a string

    Strips a given character from the beginning and end of a string (if present)

    Args:
        s (str): the base string
        c (str): the character to be stripped

    Returns:
        str: the string with characters stripped
    """
    if s.endswith(c):
        s = s[:-1]
    if s.startswith(c):
        s = s[1:]
    return s


def quote_fqn(fqn: str) -> str:
    """Quote a fully qualified name

    Takes an unquoted FQN and properly quotes it.

    Args:
        fqn (str): fully qualified name

    Returns:
        str: quoted fully qualified name

    """
    name_parts = list(fqn.split("."))
    return ".".join([f'"{clean_up_asset_name(x)}"' for x in name_parts])


def fqn(*args) -> str:
    """Create a quoted fully qualified name from a list of arguments

    Args:
        *args: Components to be used in the fqn. For exmaple,
          ["database_name", "schema_name"]

    Returns:
        str: quoted fully qualified name of the asset

    """
    return ".".join([f'"{clean_up_asset_name(x)}"' for x in args])


def run_with_progress_bar(f, my_iter, max_workers: int) -> list[any]:
    """Run a function with a progress bar

    Run function f over the iterator using max_workers number of workers.
    This function uses tqdm to provide a nice progress bar.

    Args:
        f (function): function to run
        my_iter (iterable): iterable to iterate over
        max_workers (int): number of workers to use

    Returns:
        list[any]: list of results

    """

    l = len(my_iter)
    with tqdm(total=l) as pbar:
        # let's give it some more threads:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(f, arg): arg for arg in my_iter}
            results = []
            for future in as_completed(futures):
                arg = futures[future]
                results.append(future.result())
                pbar.update(1)
    return results


class Queryable:
    """Represents classes that run queries"""

    query: str


class CustomQuery(Queryable):
    """Used to specify custom queries when a Queryable is needed"""

    query: str

    def __init__(self, query: str) -> None:
        """New CustomQuery instance

        Args:
            query: the query associated with the instance
        """
        self.query = query
