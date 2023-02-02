"""CLI related functions and utilities"""

import os
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.validator import PathValidator
import argparse
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from jetty_scorecard.util import DEFAULT_MAX_WORKERS


class TextFormat:
    """Codes to enable text formatting"""

    ORANGE = "\033[38;5;208m"
    LIGHT_GRAY = "\033[38;5;249m"
    BOLD = "\033[1m"
    ITALIC = "\033[3m"
    RESET = "\033[0m"


def parse_cli_args() -> argparse.Namespace:
    """
    Parse CLI arguments, including:
    - account
    - username
    - role
    - warehouse
    - concurrency
    - password
    - key
    - passphrase
    - sso
    - dummy
    - load
    - dump

    """
    parser = argparse.ArgumentParser(
        prog="jetty_scorecard",
        description="""Evaluate your Snowflake configuration and provide
        suggestions about how to improve your configuration""",
        epilog="""Reach out to isaac@get-jetty.com with any questions,
        comments, or suggestions!""",
    )

    parser.add_argument("-v", "--version", action="version", version="%(prog)s 0.1.5")

    details_group = parser.add_argument_group(
        "connection information", "basic information for the Snowflake connection"
    )
    details_group.add_argument(
        "-a",
        "--account",
    )
    details_group.add_argument(
        "-u",
        "--username",
        help="login name for the snowflake user",
    )
    details_group.add_argument(
        "-r",
        "--role",
        help="the snowflake role the application should use",
    )
    details_group.add_argument(
        "-w",
        "--warehouse",
        help="""the warehouse that queries should be run with; Note that many, \
but not all queries will run without a warehouse.""",
    )
    details_group.add_argument(
        "-o",
        "--output",
        help=(
            "the location in which to save the scorecard output (should be an html"
            " file)"
        ),
    )
    details_group.add_argument(
        "-c",
        "--concurrency",
        help="the number of snowflake queries to run concurrently",
        default=DEFAULT_MAX_WORKERS,
        type=int,
    )

    auth_group = parser.add_argument_group(
        "authentication",
        (
            "information needed to authenticate with Snowflake\nonly one of the"
            " following may be used"
        ),
    )

    mutually_exclusive_auth_group = auth_group.add_mutually_exclusive_group()
    mutually_exclusive_auth_group.add_argument(
        "-p",
        "--password",
        help="""password for the provided Snowflake account""",
    )

    mutually_exclusive_auth_group.add_argument(
        "-k",
        "--key",
        help="path to the private key used to authenticate with Snowflake",
    )

    auth_group.add_argument(
        "-kp",
        "--passphrase",
        help="""passphrase to decrypt private key used to authenticate with Snowflake""",
    )

    mutually_exclusive_auth_group.add_argument(
        "-s",
        "--sso",
        help="authenticate with sso (will open in a browser)",
        action="store_true",
    )

    mutually_exclusive_auth_group.add_argument(
        "-d",
        "--dummy",
        help="""use a dummy database - don't actually run any queries""",
        action="store_true",
    )

    dev_group = parser.add_argument_group(
        "development",
        "helpers for development",
    )

    dev_group.add_argument(
        "--load",
        help="""path from which to load a pickled environment; \
use with -d to skip the authentication flow""",
    )

    dev_group.add_argument(
        "--dump",
        help="""path to which to dump a pickled environment \
(directly after fetching metadata, but before loading any checks)""",
    )

    return parser.parse_args()


def run_interactive_prompt(args: argparse.Namespace) -> tuple[dict[str, str], str]:
    """
    Run the interactive prompt for the CLI.

    Args:
        args: the parsed CLI arguments
    Returns:
        a tuple of the credentials and the CLI string that can be used
        to generate the scorecard, skipping the walkthrough
    """

    credentials = {}
    key_path = None
    has_passphrase = False

    if args.dummy:
        return credentials, generate_cli_for_next_time({})

    if args.account is None:
        credentials["account"] = inquirer.text(
            message="Enter your account identifier:",
            mandatory=True,
            validate=lambda result: len(result) > 0,
            invalid_message="Input cannot be empty.",
            long_instruction="""\nThis is typically the part before \
'.snowflakecomputing.com' in your snowflake URL. You can read more about the \
different types of Snowflake account identifiers at \
https://docs.snowflake.com/en/user-guide/admin-account-identifier.html.""",
        ).execute()
    else:
        credentials["account"] = args.account

    if args.username is None:
        credentials["user"] = inquirer.text(
            message="Enter your username:",
            mandatory=True,
            validate=lambda result: len(result) > 0,
            invalid_message="Input cannot be empty.",
            long_instruction=(
                "\nThis is the name or email address you use to log into Snowflake."
            ),
        ).execute()
    else:
        credentials["user"] = args.username

    if args.role is None:
        credentials["role"] = inquirer.text(
            message="Enter your role:",
            mandatory=True,
            validate=lambda result: len(result) > 0,
            invalid_message="Input cannot be empty.",
            long_instruction=(
                "\nThis is the role you'd like to use to generate your scorecard. For"
                " the most complete view of your environment, use an administrator role"
                " such as SECURITYADMIN or ACCOUNTADMIN."
            ),
        ).execute()
    else:
        credentials["role"] = args.role

    if args.warehouse is None:
        credentials["warehouse"] = inquirer.text(
            message="Enter your warehouse:",
            mandatory=True,
            validate=lambda result: len(result) > 0,
            invalid_message="Input cannot be empty.",
            long_instruction=(
                "\nThis is the warehouse you would like to use to generate your"
                " scorecard. Many of the queries are metadata queries (beginning with"
                " the SHOW keyword), so run without a warehouse. Some queries, however,"
                " need to read from tables so require a warehouse."
            ),
        ).execute()
    else:
        credentials["warehouse"] = args.warehouse

    if args.password:
        credentials["password"] = args.password
    elif args.key:
        key_path = args.key
        has_passphrase = args.passphrase is not None
        credentials["private_key"] = get_private_key(key_path, args.passphrase)
    elif args.sso:
        credentials["authenticator"] = "externalbrowser"
    else:
        authentication_method = inquirer.select(
            message="Choose your authentication method:",
            choices=[
                Choice(value="sso", name="SSO (will open in a browser)"),
                Choice(
                    value="key",
                    name=(
                        "Key Pair (you can read more here:"
                        " https://docs.snowflake.com/en/user-guide/key-pair-auth.html)"
                    ),
                ),
                Choice(value="password", name="Password"),
                Choice(value="dummy", name="No Authentication"),
            ],
            mandatory=True,
            default="sso",
            wrap_lines=True,
            long_instruction="""\nIn this next section you will be prompted for credentials \
to authenticate with Snowflake. These credentials are not sent to anyone, or \
even written to disk - they are only used for the duration of this program's \
execution execution.
If you would prefer not to authenticate, you can choose "No Authentication" \
to see information about the checks Jetty Scorecard runs \
as well as links to documentation about best practices.""",
        ).execute()

        if authentication_method == "dummy":
            return {}, generate_cli_for_next_time({})

        elif authentication_method == "password":
            credentials["password"] = inquirer.secret(
                f"Enter the password for {credentials['user']}:",
                mandatory=True,
                validate=lambda result: len(result) > 0,
                invalid_message="Input cannot be empty.",
            ).execute()

        elif authentication_method == "sso":
            credentials["authenticator"] = "externalbrowser"

        elif authentication_method == "key":
            home_path = "~/.ssh" if os.name == "posix" else "C:\\"
            key_path = inquirer.filepath(
                message="Enter the path to your private file:",
                default=home_path,
                validate=PathValidator(is_file=True, message="Input is not a file"),
                mandatory=True,
            ).execute()
            key_path = os.path.expanduser(key_path)

            with open(key_path, "r") as key_file:
                is_encrypted = "ENCRYPTED" in key_file.readline()
            passphrase = None
            if is_encrypted:
                passphrase = inquirer.secret(
                    f"Enter the passphrase for your private key:",
                    mandatory=True,
                    validate=lambda result: len(result) > 0,
                    invalid_message="Input cannot be empty.",
                ).execute()
                has_passphrase = True

            credentials["private_key"] = get_private_key(key_path, passphrase)

    return credentials, generate_cli_for_next_time(credentials, has_passphrase)


def prompt_for_output_location(args: argparse.Namespace) -> str:
    """
    Prompts the user for an output location.

    Args:
        args (argparse.Namespace): The parsed command line arguments.

    Returns:
        str: The output location.
    """
    if args.output is None:
        return inquirer.text(
            message="Enter output location:",
            mandatory=True,
            validate=lambda result: len(result) > 0,
            invalid_message="Input cannot be empty.",
            long_instruction=(
                "\nThis is the location where the scorecard will be saved. It should be"
                " an html file."
            ),
            default="./jetty_scorecard.html",
        ).execute()
    else:
        return args.output


def get_private_key(key_path: str, passcode: str | None):
    """
    Prompts the user for a private key.

    Args:
        key_path (str): The path to the private key file.
        passcode (str | None): The passphrase to decrypt the private key.
          If None, assume an unencrypted key.

    Returns:
        str: The private key.
    """
    with open(key_path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(), password=passcode, backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def generate_cli_for_next_time(
    credentials: dict, key_path: str = None, has_passphrase: bool = False
) -> str:
    """
    Generates a CLI command to run the program again for the same environment.

    Args:
        credentials (dict): The credentials collected from the interactive
                            prompt or cli.
    """
    if len(credentials) == 0:
        return "jetty_scorecard -d -o <desired_output_file.html>"

    credentials_string = ""
    if credentials.get("password"):
        credentials_string = "-p ***your_password***"
    elif credentials.get("private_key"):
        passphrase_part = " -kp ***your_passphrase***" if has_passphrase else ""
        credentials_string = f"-k {key_path}{passphrase_part}"
    elif credentials.get("authenticator"):
        credentials_string = "-s"

    return f"""jetty_scorecard -a {credentials['account']} -u {credentials['user']} -r {credentials['role']} -w {credentials['warehouse']} {credentials_string} -o <desired_output_file.html>"""


def welcome_message():
    """Prints a welcome message."""
    print(
        f"""Welcome to the {TextFormat.ORANGE}{TextFormat.BOLD}Jetty Scorecard CLI!!{TextFormat.RESET}\n
Let's get started...\n"""
    )


def print_cli_command(command: str):
    """Print the CLI command that can be used to run the application next time"""
    print(
        f"""\nTo skip the configuration wizard, next time just run:
  {TextFormat.ITALIC}{TextFormat.LIGHT_GRAY}{command}{TextFormat.RESET}\n"""
    )
