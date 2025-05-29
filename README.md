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
