<p align="center">
  <img src="./etc/scorecard_logo.svg" alt="jetty scorecard logo" width="700" >
</p>
<br><br>

# About Jetty Scorecard

It can be hard to keep track of, not to mention follow, data infrastructure best practices - we want to change that!

Jetty Scorecard is living documentation of Snowflake best practices - it outlines recommendations with links to relevant documentation, and does so in the context of your existing environment. This makes it easy to know what you can do right now to improve your security and maintainability.

# Getting Started

### Installation

~~Install Jetty Scorecard with~~ (coming soon - for now just clone the repo and run poetry install)

```bash
pip install **********
```

### Running Jetty Scorecard

Once installed, you can launch it by simply running

```bash
jetty_scorecard
```

This will take you through an interactive connection setup to connect to your Snowflake instance. You can authenticate using browser-based SSO, a password, or a private key.

> **_NOTE:_** If you don't want to connect to Snowflake, you can still see the the best practices Scorecard looks for. Just run `jetty_scorecard -d` to generate a dummy scorecard to show an example scorecard (without any actual scores).

Once you've authenticated with Snowflake, Scorecard will run for a few moments and then open up a browser with your results!

### Interpreting your results

Each result includes:

-   A description of what is being checked
-   Links to relevant documentation
-   The queries used to run the check
-   Specific insights into your roles, users, polices, and permissions to help you improve your configuration

### Getting for updated results

After making changes to your Snowflake environment, you can run Scorecard again. You can choose to go through the wizard again, or, if you'd rather, you can include all of the relevant information on the command line (you can even do this the first time around!).

After you run Scorecard the first time, it will print to your console a command that you can use to generate the scorecard again using the same configuration. You can also just read the CLI documentation by running

```bash
jetty_scorecard --help
```

> If you think this is neat, give it a star, and share it with someone you know!

# Contributing

This is just a first draft of Scorecard - there's so much more that can be done!

Do you have ideas for additional best practices that should be included? Would you like to modify some that already exist? Do you have suggestions for the UI or UX?

If you have suggestions or comments, feel free to create an issue, open a pull request, or reach out in another way!

# Security considerations

Unless run in dummy mode (`jetty_scorecard -d`) Jetty Scorecard pulls metadata from your database, and so requires a database connection. To get holistic account-level data, it should be run with an administrator role, such as SECURITYADMIN or ACCOUNTADMIN (or another role with similar, though perhaps read-only privileges).

To provide peace of mind, it is worth noting that the Jetty Scorecard application:

-   Does not capture and/or share usage analytics of any sort
-   Does not write any credentials to disk
-   Runs exclusively read-only queries

# About Jetty Labs

Jetty Labs is building software to simplify access management across the modern data stack. You can see more about what we're working on at https://www.get-jetty.com.

<br>
<p align="center">
<img src="./etc/logo.svg" alt="jetty scorecard logo" width="50" >
</p>
