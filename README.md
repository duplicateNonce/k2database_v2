# K2Database v2

This project uses environment variables loaded from a `.env` file. An example for enabling multiple login users:

```
# .env
APP_USERS=alice:pass1,bob:pass2
```

Each entry uses `username:password` pairs separated by commas.
Spaces around commas are ignored, so the following is also valid:

```
# .env
APP_USERS=alice:pass1, bob:pass2
```

On Streamlit Cloud you may also provide the same values in
`.streamlit/secrets.toml`:

```
[app_users]
alice = "pass1"
bob   = "pass2"
```

Environment variables still take precedence if both are provided.

## Requirements

Install dependencies with `pip`:
```bash
pip install -r requirements.txt
```

The `pysocks` package is included to support SOCKS proxies.


## Database settings

The app connects to PostgreSQL using the host, port, username and
password defined by several environment variables.  Defaults are
set in `config.py`, but you can override them locally using a `.env`
file or on Streamlit Cloud via `secrets.toml`.

Common variables:

```
DB_HOST     # database address, e.g. 127.0.0.1 for local testing
DB_PORT     # usually 5432
DB_USER     # database user
DB_PASSWORD # database password
TRON_DB_NAME # database used by tron_bot.py (defaults to DB_NAME)
```

For example, a local `.env` could look like:

```env
DB_HOST=127.0.0.1
DB_USER=postgres
DB_PASSWORD=your_local_password
```

When deploying to Streamlit Cloud, provide the same variables in the
app's **Secrets** so it can reach your server:

```toml
DB_HOST = "YOUR-SERVER-IP"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "cloud_password"
```

If your database listens only on `127.0.0.1` you'll need to update
`postgresql.conf` so it binds to a public interface and allows remote
connections.

## Telegram alerts

Some scripts send notifications via a Telegram bot.  Provide the bot token
and the target chat/group ID using environment variables:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

You can also define the same variables in `secrets.toml` when deploying to
Streamlit Cloud.  If these variables are missing, the helper will print the
message to stdout instead of silently ignoring it so local testing is easier.

Messages containing ASCII tables are sent using Markdown code blocks so they
align properly in Telegram.  If you send your own messages with code-style
formatting, pass ``parse_mode="Markdown"`` to ``send_message``.

## Streamlit notes

This project runs on a minimal Streamlit build that does **not** support
`st.experimental_rerun`.  Use the helper `safe_rerun()` from `utils.py`
instead of calling the experimental API directly.

Streamlit has also deprecated `st.experimental_get_query_params` in favor of
`st.query_params`.  The codebase already uses the new API, so contributors
should avoid the old function to prevent deprecation warnings.



When a user logs in for the first time a random **device ID** is
generated (using ``uuid4``).  The ID is stored in ``data/tokens.json`` and
in the browser's ``localStorage``.  It is also appended to the page URL
as the ``tok`` query parameter so returning visits can skip the login
form automatically.  Logging in from another device replaces the
previous ID, immediately invalidating the old session.


For best security, deploy the app behind HTTPS so the browser does not
flag the page as insecure.

### 4h aggregation

The monitoring bot combines 15 minute candles into 4 hour bars. Sixteen
consecutive 15 minute records (00, 15, 30 and 45 minutes past the hour)
are required for a valid bar. The aggregated candle uses the first
record's open, the highest high and lowest low of the group, the last
record's close and the sum of all volumes. Partial periods (for example if
data ends on the 30 minute mark) are discarded. Aggregation starts from
00:00 so the final bar of a day is 20:00–24:00.

Aggregated 4h candles are cached under `data/cache/4h` as CSV files to
avoid recomputing from raw 15 minute data. Each query updates the cache
with any new records and the bot keeps aggregated results in memory to
speed up repeated checks.
