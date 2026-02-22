# MinIO-Telegram: S3-Compatible Storage with Telegram

[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![license](https://img.shields.io/badge/license-AGPL%20V3-blue)](https://github.com/minio/minio/blob/master/LICENSE)

This project is a modified version of the MinIO Object Storage server that uses **Telegram** as its storage backend. It allows you to have a fully S3-compatible object storage interface while the actual data is stored in a Telegram channel, effectively providing a massive, low-cost storage solution.

## How It Works

This implementation uses a decorator pattern to wrap MinIO's native object layer.

-   **S3 API**: All S3 API requests are handled by the battle-tested MinIO engine.
-   **Data Storage**: Large file objects are chunked and uploaded to a specified Telegram channel. Small files (< 500KB) are stored directly in the metadata database to optimize performance.
-   **Metadata**: Object metadata, bucket information, and the location of file chunks in Telegram are stored in a **PostgreSQL** database.
-   **System Buckets**: Internal MinIO system buckets (like `.minio.sys`) are handled by the native filesystem layer, ensuring the Web UI and internal operations function correctly.

## Features

-   Fully S3-compatible API.
-   "Unlimited" storage capacity (bound by Telegram's limits).
-   Handles large files by automatically splitting them into 1.9GB chunks.
-   Optimized downloads that stream directly from Telegram without requiring large temporary files.
-   PostgreSQL backend for robust metadata management.

## Setup and Installation

### Prerequisites

-   Go 1.21+
-   A running PostgreSQL database.
-   Telegram App credentials (`API_ID`, `API_HASH`) from [my.telegram.org](https://my.telegram.org).
-   A Telegram Bot Token (from [@BotFather](https://t.me/BotFather)).
-   A Telegram Channel ID where the bot is an administrator.

### 1. Build the Binary

From within the `minio` directory, run the following command:

```bash
go build -o minio-telegram .
```

### 2. Configure Environment Variables

The server is configured entirely through environment variables.

| Variable                | Description                                                                                             | Example                                                              |
| ----------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `MINIO_TELEGRAM_ENABLED`  | Must be set to `on` to activate the Telegram backend.                                                     | `on`                                                                 |
| `TELEGRAM_API_ID`         | Your Telegram application API ID.                                                                       | `12345678`                                                           |
| `TELEGRAM_API_HASH`       | Your Telegram application API hash.                                                                     | `0123456789abcdef0123456789abcdef`                                   |
| `TELEGRAM_BOT_TOKEN`      | Your Telegram bot token.                                                                                | `1234567890:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`                      |
| `TELEGRAM_CHANNEL_ID`     | The ID of the channel to store files in. Must start with `-100`.                                        | `-1001234567890`                                                     |
| `POSTGRES_URL`          | The full connection string for your PostgreSQL database.                                                | `postgresql://user:password@host:5432/database`                      |
| `MINIO_ROOT_USER`         | The root username for the MinIO S3 API and console.                                                     | `minioadmin`                                                         |
| `MINIO_ROOT_PASSWORD`     | The root password for the MinIO S3 API and console.                                                     | `minioadmin`                                                         |
| `TG_PROXY` (Optional)     | An MTProto proxy to connect to Telegram.                                                                | `tg://proxy?server=...&port=...&secret=...`                          |

### 3. Run the Server

Execute the binary with a path for local MinIO system data and a console address. The data path is for internal system buckets, not your objects.

```bash
# Example command
MINIO_TELEGRAM_ENABLED="on" \
TELEGRAM_API_ID="your_id" \
TELEGRAM_API_HASH="your_hash" \
TELEGRAM_BOT_TOKEN="your_token" \
TELEGRAM_CHANNEL_ID="-100xxxxxxxxxx" \
POSTGRES_URL="postgresql://user:pass@host:5432/db" \
MINIO_ROOT_USER="minioadmin" \
MINIO_ROOT_PASSWORD="minioadmin" \
./minio-telegram server /tmp/minio-data --console-address ":9001"
```

Once running, you can access the MinIO console at `http://127.0.0.1:9001`.

## Usage

You can interact with the server using any S3-compatible tool, including the MinIO Client (`mc`).

```bash
# 1. Alias your new server
mc alias set my-telegram-s3 http://127.0.0.1:9000 minioadmin minioadmin

# 2. Make a bucket
mc mb my-telegram-s3/my-first-bucket

# 3. Upload a file
mc cp /path/to/your/file.txt my-telegram-s3/my-first-bucket/

# 4. List the contents of the bucket
mc ls my-telegram-s3/my-first-bucket/
```

## Contributing

Please follow MinIO [Contributor's Guide](https://github.com/minio/minio/blob/master/CONTRIBUTING.md) for guidance on making new contributions.

## License

This project is based on MinIO and is licensed under the [GNU AGPLv3](https://github.com/minio/minio/blob/master/LICENSE).
