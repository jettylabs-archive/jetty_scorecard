"""Utility functions for building a scorecard"""

from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from enum import Enum, auto
from jinja2 import Environment, BaseLoader, PackageLoader

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


def add_missing_quotes_to_fqn(fqn: str) -> str:
    """Add quotes to a fully qualified name, but only if necessary

    Takes an optionally quoted FQN and properly quotes it.

    Args:
        fqn (str): fully qualified name

    Returns:
        str: quoted fully qualified name

    """
    name_parts = list(fqn.split("."))
    return ".".join([f'"{x}"' if not x.startswith('"') else x for x in name_parts])


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


def truncated_table(fqn: str) -> str | None:
    """Truncate a fully qualified name to its table

    Args:
        fqn (str): fully qualified asset name

    Returns:
        str | None: fully qualified table name or None if no table was found

    """
    split_name = fqn.split('"."')[:3]
    if len(split_name) != 3:
        return None
    else:
        partial_name = '"."'.join(split_name)
        if not partial_name.endswith('"'):
            partial_name += '"'
        return partial_name


def truncated_schema(fqn: str) -> str | None:
    """Truncate a fully qualified name to its schema

    Args:
        fqn (str): fully qualified asset name

    Returns:
        str | None: fully qualified schema name or None if no table was found

    """
    split_name = fqn.split('"."')[:2]
    if len(split_name) != 2:
        return None
    else:
        partial_name = '"."'.join(split_name)
        if not partial_name.endswith('"'):
            partial_name += '"'
        return partial_name


def extract_schema(fqn: str) -> str | None:
    """Extract the schema from a fully qualified name

    Args:
        fqn (str): fully qualified asset name

    Returns:
        str | None: schema name or None if no schema was found

    """
    truncated = truncated_schema(fqn)
    if truncated is None:
        return None
    else:
        return f""""{truncated.split('"."')[-1]}"""


def truncated_database(fqn: str) -> str | None:
    """Truncate a fully qualified name to its database

    Args:
        fqn (str): fully qualified asset name

    Returns:
        str | None: fully qualified database name or None if no table was found

    """
    split_name = fqn.split('"."')[:1]
    if len(split_name) != 1:
        return None
    else:
        partial_name = split_name[0]
        if not partial_name.endswith('"'):
            partial_name += '"'
        return partial_name


class FQNType(Enum):
    """Enum to classify the asset type of a fully qualified name"""

    DATABASE = auto()
    SCHEMA = auto()
    TABLE = auto()


def fqn_type(fqn: str) -> FQNType:
    """Classify the asset type of a fully qualified name

    Args:
        fqn (str): fully qualified asset name

    Returns:
        FQNType: the type of the asset

    """
    num_segments = len(fqn.split('"."'))

    if num_segments == 1:
        return FQNType.DATABASE
    elif num_segments == 2:
        return FQNType.SCHEMA
    elif num_segments == 3:
        return FQNType.TABLE
    else:
        raise Exception(f"{fqn} is not a valid fully qualified name")


def render_string_template(template: str, context: any) -> str:
    """Render a string template

    Args:
        template (str): the template to render
        context (any): the context to render the template with

    Returns:
        str: the rendered template

    """
    jinja_template = Environment(loader=BaseLoader()).from_string(template)
    return jinja_template.render(context)


def render_check_template(template_name: str, context: any) -> str:
    """Render a stored template

    Args:
        template_name (str): the template to render. This should be saved
          in the `checks/templates` directory
        contex (any): the context to render the template with

    Returns:
        str: the rendered template

    """
    jinja_env = Environment(loader=PackageLoader("jetty_scorecard.checks"))
    template = jinja_env.get_template(template_name)

    return template.render(context)
