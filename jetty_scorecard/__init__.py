from jetty_scorecard import cli
from jetty_scorecard.env import SnowflakeEnvironment
from jetty_scorecard.checks import all_checks
from pathlib import Path
import pickle
import webbrowser
from copy import deepcopy

import jinja2


def run():
    """Run the CLI.

    Run the cli by:
    - parsing cli args
    - prompting for credentials
    - running checks
    - writing output to file

    Returns:
        None
    """
    args = cli.parse_cli_args()

    cli.welcome_message()
    credentials, cli_command = cli.run_interactive_prompt(args)
    output_path = cli.prompt_for_output_location(args)

    if args.load:
        with open(args.load, "rb") as file_handle:
            env = pickle.load(file_handle)
    else:
        env = SnowflakeEnvironment(args.concurrency)

    if len(credentials) > 0:
        env.connect(credentials)
        env.fetch_environment()

    if args.dump:
        with open(args.dump, "wb") as file_handle:
            pickle.dump(env.copy(), file_handle)

    all_checks.register(env)

    env.run_checks()

    cli.print_cli_command(cli_command)

    write_output_file(output_path, env.html)

    webbrowser.open_new_tab(Path(output_path).resolve().as_uri())


def write_output_file(output_path: str, content: str):
    """Write "content" to the specified output_path.

    Create any necessary directories and then write the data in content
    to the specified output path.

    Args:
        output_path (str): The path to the output file.
        content (str): The content to write to the file.

    Returns:
        None
    """
    Path(output_path).resolve().parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(content)
