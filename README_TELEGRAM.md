# MinIO Telegram Storage Gateway

This modification allows MinIO to use Telegram as an object storage backend, with Redis handling metadata.

## Prerequisites

- **Go 1.21+**
- **Redis** running locally or accessible.
- **Telegram App credentials** (`API_ID`, `API_HASH`) from [telegram.org](https://my.telegram.org).
- **Telegram Bot Token** (from @BotFather).
- **Target Channel ID** (where files will be stored). The bot must be an admin in this channel.

## Setup

1.  **Dependencies**:
    Run the following in the `minio` directory to download required modules:
    ```bash
    go get github.com/gotd/td/telegram
    go get github.com/go-redis/redis/v8
    go mod tidy
    ```

2.  **Environment Variables**:
    Create a script or export these variables:

    ```bash
    export MINIO_TELEGRAM_ENABLED=on
    export TELEGRAM_API_ID=your_api_id
    export TELEGRAM_API_HASH=your_api_hash
    export TELEGRAM_BOT_TOKEN=your_bot_token
    export TELEGRAM_CHANNEL_ID=-100xxxxxxxxxx  # Ensure it starts with -100 for channels
    export REDIS_URL=localhost:6379
    export REDIS_PASSWORD=
    export REDIS_DB=0
    
    # MinIO Standard Envs (Optional)
    export MINIO_ROOT_USER=minioadmin
    export MINIO_ROOT_PASSWORD=minioadmin
    ```

3.  **Build**:
    ```bash
    cd minio/cmd # or just minio
    go build -o minio_tg .
    ```

4.  **Run**:
    ```bash
    ./minio_tg server /tmp/data
    ```
    *(Note: `/tmp/data` is just a placeholder, the storage backend will bypass disk and use Telegram)*

## Usage

Use `mc` (MinIO Client) to interact with the gateway:

```bash
mc alias set mytg http://localhost:9000 minioadmin minioadmin
mc mb mytg/testbucket
mc cp myphoto.jpg mytg/testbucket/
mc ls mytg/testbucket/
mc rm mytg/testbucket/myphoto.jpg
```

## Implementation Details

- **Metadata**: Stored in Redis keys:
    - Buckets: `minio:buckets` (Set)
    - Objects: `minio:object:<bucket>:<key>` (JSON String)
- **Storage**: Files are uploaded to the specified Telegram channel.
- **Limit**: Currently supports files up to ~1.9GB (single chunk). Multipart upload is stubbed.

## Limitations

- **Multipart Uploads**: Not fully implemented.
- **Range Requests**: Not fully optimized.
- **Authentication**: Uses Bot API (via `gotd`). Ensure the bot has rights to upload to the channel.
