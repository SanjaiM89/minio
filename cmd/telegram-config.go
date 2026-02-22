package cmd

import (
	"os"
	"strconv"
)

// TelegramConfig holds the configuration for the Telegram backend
type TelegramConfig struct {
	AppID           int
	AppHash         string
	BotToken        string
	ChannelID       int64
	BareChannelID   int64
	PostgresURL     string
	ProxyURL        string
	TelegramEnabled bool
}

// LoadTelegramConfig loads the Telegram configuration from environment variables
func LoadTelegramConfig() *TelegramConfig {
	enabled := os.Getenv("MINIO_TELEGRAM_ENABLED") == "on"

	appID, _ := strconv.Atoi(os.Getenv("TELEGRAM_API_ID"))
	channelID, _ := strconv.ParseInt(os.Getenv("TELEGRAM_CHANNEL_ID"), 10, 64)

	bareChannelID := channelID
	if bareChannelID <= -1000000000000 {
		bareChannelID = -(bareChannelID + 1000000000000)
	} else if bareChannelID < 0 {
		bareChannelID = -bareChannelID
	}

	return &TelegramConfig{
		AppID:           appID,
		AppHash:         os.Getenv("TELEGRAM_API_HASH"),
		BotToken:        os.Getenv("TELEGRAM_BOT_TOKEN"),
		ChannelID:       channelID,
		BareChannelID:   bareChannelID,
		PostgresURL:     os.Getenv("POSTGRES_URL"),
		ProxyURL:        os.Getenv("TG_PROXY"),
		TelegramEnabled: enabled,
	}
}
