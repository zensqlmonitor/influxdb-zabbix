package log

import (
	"fmt"
	"strings"

	cfg "github.com/zensqlmonitor/influxdb-zabbix/config"
)

// Initialize logging
func Init(config cfg.TOMLConfig) {
	var LogModes []string
	var LogConfigs []string

	// Log Modes
	LogModes = strings.Split(config.Logging.Modes, ",")
	LogConfigs = make([]string, len(LogModes))

	for i, mode := range LogModes {
		mode = strings.TrimSpace(mode)

		// Log Level
		var levelName string
		if mode == "console" {
			levelName = config.Logging.LevelConsole
		} else {
			levelName = config.Logging.LevelFile
		}

		level, ok := LogLevels[levelName]
		if !ok {
			Fatal(4, "Unknown log level: %s", levelName)
		}
		// Generate log configuration
		switch mode {
		case "console":
			LogConfigs[i] = fmt.Sprintf(`{"level":%v,"formatting":%v}`,
				level,
				config.Logging.Formatting)
		case "file":
			LogConfigs[i] = fmt.Sprintf(`{"level":%v,"filename":"%s","rotate":%v,"maxlines":%d,"maxsize":%d,"daily":%v,"maxdays":%d}`,
				level,
				config.Logging.FileName,
				config.Logging.LogRotate,
				config.Logging.MaxLines,
				1<<uint(config.Logging.MaxSizeShift),
				config.Logging.DailyRotate,
				config.Logging.MaxDays)
		}
		NewLogger(int64(config.Logging.BufferLen), mode, LogConfigs[i])
		Trace("Log Mode: %s(%s)", strings.Title(mode), levelName)
	}
}
