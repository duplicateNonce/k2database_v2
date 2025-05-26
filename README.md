# K2Database v2

This project uses environment variables loaded from a `.env` file. An example for enabling multiple login users:

```
# .env
APP_USERS=alice:pass1,bob:pass2
```

Each entry uses `username:password` pairs separated by commas.
