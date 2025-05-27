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

## Streamlit notes

This project runs on a minimal Streamlit build that does **not** support
`st.experimental_rerun`.  Use the helper `safe_rerun()` from `utils.py`
instead of calling the experimental API directly.

When a user logs in successfully a fingerprint token is stored in
`data/fingerprints.json`.  The token is also written to the URL as a
query parameter.  If the browser reloads the app with this token it will
log in automatically.  Each account is limited to a single fingerprint;
attempting to log in from another browser yields `ERROR 01`.
