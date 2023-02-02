from jetty_scorecard import cli
from jetty_scorecard.env import SnowflakeEnvironment
from jetty_scorecard.checks import all_checks
from pathlib import Path
import pickle
import webbrowser


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
        # If something goes wrong fetching the environment, we'll still run the checks,
        # but it will be as if we ran them with -d
        try:
            env.fetch_environment()
        except Exception as e:
            print(f"Unable to fetch environment: {e}")
            env = SnowflakeEnvironment(args.concurrency)

    if args.dump:
        write_output_file(args.dump, pickle.dumps(env.copy()), "wb")

    all_checks.register(env)

    env.run_checks()

    cli.print_cli_command(cli_command)

    write_output_file(output_path, env.html)

    webbrowser.open_new_tab(Path(output_path).resolve().as_uri())


def write_output_file(output_path: str, content: str, mode: str = "w"):
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
    with open(output_path, mode) as f:
        f.write(content)
