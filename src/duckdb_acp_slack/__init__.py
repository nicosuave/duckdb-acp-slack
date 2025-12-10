"""Slack bot that queries Claude Code via the DuckDB ACP extension."""

import os
import re
from pathlib import Path
from typing import Optional

import duckdb
import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Auto-load .env files (later files override earlier)
for env_file in [".env", ".env.local"]:
    if Path(env_file).exists():
        load_dotenv(env_file, override=True)

console = Console()
app_typer = typer.Typer(add_completion=False)

# Global config set by CLI
_config: dict = {}


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with configured databases and init SQL."""
    conn = duckdb.connect()

    # Install and load ACP extension
    conn.execute("INSTALL acp FROM community;")
    conn.execute("LOAD acp;")

    # Attach databases
    for name, path in _config.get("databases", {}).items():
        conn.execute(f"ATTACH '{path}' AS {name} (READ_ONLY);")

    # Run init SQL
    if _config.get("init_sql"):
        conn.execute(_config["init_sql"])

    return conn


def query_claude(prompt: str) -> tuple[str, str | None]:
    """Query Claude via DuckDB ACP extension. Returns (message, csv_content)."""
    conn = get_connection()

    try:
        result = conn.execute(f"CLAUDE {prompt};")
        columns = [col[0] for col in result.description]
        rows = result.fetchall()

        if not rows:
            return "_No results_", None

        # Build CSV
        csv_lines = [",".join(columns)]
        for row in rows:
            csv_lines.append(",".join(str(v) if v is not None else "" for v in row))
        csv_content = "\n".join(csv_lines)

        msg = f"Returned {len(rows)} row(s), {len(columns)} column(s)"
        return msg, csv_content

    except Exception as e:
        return f"*Error:* `{e}`", None
    finally:
        conn.close()


def create_app(bot_token: str) -> App:
    """Create and configure the Slack app."""
    app = App(token=bot_token)

    @app.event("app_mention")
    def handle_mention(event, say, client):
        text = event.get("text", "")
        channel = event["channel"]
        ts = event["ts"]
        thread_ts = event.get("thread_ts", ts)

        try:
            client.reactions_add(channel=channel, timestamp=ts, name="eyes")
        except Exception:
            pass

        prompt = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()

        if not prompt:
            say(
                text="Please include a question or query after mentioning me.",
                channel=channel,
                thread_ts=thread_ts,
            )
            return

        console.print(f"[dim]{channel}[/dim] [yellow]→[/yellow] {prompt[:80]}{'...' if len(prompt) > 80 else ''}")

        ack_response = client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f"Working on: _{prompt}_",
        )

        msg, csv_content = query_claude(prompt)

        client.chat_update(
            channel=channel,
            ts=ack_response["ts"],
            text=f"*Query:* _{prompt}_\n\n{msg}",
        )

        if csv_content:
            client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                content=csv_content,
                filename="results.csv",
                title="Query Results",
            )
            console.print(f"[dim]{channel}[/dim] [green]✓[/green] {msg}")
        else:
            console.print(f"[dim]{channel}[/dim] [green]✓[/green] done")

    @app.event("message")
    def handle_message(event, say, client, logger):
        if event.get("bot_id") or event.get("subtype"):
            return

        text = event.get("text", "")
        if re.search(r"<@[A-Z0-9]+>", text):
            return

        channel = event["channel"]
        ts = event["ts"]
        thread_ts = event.get("thread_ts", ts)

        prompt = text.strip()
        if not prompt:
            return

        console.print(f"[dim]{channel}[/dim] [yellow]→[/yellow] {prompt[:80]}{'...' if len(prompt) > 80 else ''}")

        ack_response = client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f"Working on: _{prompt}_",
        )

        msg, csv_content = query_claude(prompt)

        client.chat_update(
            channel=channel,
            ts=ack_response["ts"],
            text=f"*Query:* _{prompt}_\n\n{msg}",
        )

        if csv_content:
            client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                content=csv_content,
                filename="results.csv",
                title="Query Results",
            )
            console.print(f"[dim]{channel}[/dim] [green]✓[/green] {msg}")
        else:
            console.print(f"[dim]{channel}[/dim] [green]✓[/green] done")

    return app


@app_typer.command()
def main(
    bot_token: Optional[str] = typer.Option(
        None,
        "--bot-token",
        envvar="SLACK_BOT_TOKEN",
        help="Slack Bot Token (xoxb-...)",
    ),
    app_token: Optional[str] = typer.Option(
        None,
        "--app-token",
        envvar="SLACK_APP_TOKEN",
        help="Slack App Token (xapp-...)",
    ),
    db: Optional[list[Path]] = typer.Option(
        None,
        "--db",
        help="Attach database file",
    ),
    init_sql: Optional[Path] = typer.Option(
        None,
        "--init-sql",
        help="Path to SQL file to run on startup",
    ),
):
    """
    Start the DuckDB Claude Slack bot.

    Query your data in plain English from Slack.
    """
    if not bot_token:
        console.print("[red]Error:[/red] Missing --bot-token or SLACK_BOT_TOKEN")
        raise typer.Exit(1)

    if not app_token:
        console.print("[red]Error:[/red] Missing --app-token or SLACK_APP_TOKEN")
        raise typer.Exit(1)

    # Parse databases
    databases = {}
    if db:
        for path in db:
            if not path.exists():
                console.print(f"[red]Error:[/red] Database not found: {path}")
                raise typer.Exit(1)
            name = path.stem
            databases[name] = str(path)

    # Load init SQL
    init_sql_content = None
    if init_sql:
        if not init_sql.exists():
            console.print(f"[red]Error:[/red] Init SQL file not found: {init_sql}")
            raise typer.Exit(1)
        init_sql_content = init_sql.read_text()

    # Store config
    _config["databases"] = databases
    _config["init_sql"] = init_sql_content

    # Print banner
    console.print(Panel.fit(
        "[bold blue]duckdb-claude-slack[/bold blue]",
        border_style="blue",
    ))

    if databases:
        console.print("[dim]Databases:[/dim]")
        for name, path in databases.items():
            console.print(f"  [green]{name}[/green] → {path}")

    if init_sql:
        console.print(f"[dim]Init SQL:[/dim] {init_sql}")

    console.print()

    # Start bot (suppress Bolt's emoji spam)
    import logging
    logging.getLogger("slack_bolt").setLevel(logging.WARNING)

    slack_app = create_app(bot_token)
    slack_app._framework_logger.setLevel(logging.WARNING)
    handler = SocketModeHandler(slack_app, app_token, trace_enabled=False)
    console.print("[green]Listening for messages...[/green]\n")
    handler.connect()
    import time
    while True:
        time.sleep(1)


def cli():
    """Entry point for the CLI."""
    app_typer()
