# duckdb-claude-slack

Slack bot that queries Claude Code via the [DuckDB ACP extension](https://github.com/sidequery/duckdb-acp).

![screenshot](screenshot.png)

## Install

```bash
uvx duckdb-claude-slack
```

Or from source:

```bash
uvx --from git+https://github.com/nicosuave/duckdb-claude-slack duckdb-claude-slack
```

## Setup

1. Create a Slack app using the manifest in `slack_manifest.json`
2. Enable Socket Mode and generate an app token (`xapp-...`)
3. Install to workspace and copy the bot token (`xoxb-...`)
4. Set environment variables:

```bash
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
```

5. Run:

```bash
uvx duckdb-claude-slack
```

## Usage

Mention the bot in Slack:

```
@Claude DuckDB show me the top 10 customers by revenue
```

Results are returned as a CSV attachment.
