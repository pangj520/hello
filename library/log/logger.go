/**
*描述：log客户端
*      实现日志规范化输出
*      向其他文件提供写日志接口
*作者：江洪
*时间：2019-5-20
 */

package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"library_Test/common"
	"library_Test/library/utils"
	"time"

	nested "github.com/antonfisher/nested-logrus-formatter"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var Logger MyLog

var Tracer *Jaeger

type LoggerConfig struct {
	Disable             string
	ServiceName         string
	LogFilePath         string `check:"required"`
	LogOnlyStout        string `check:"required"`
	LogFormat           string `check:"required"`
	LogLevel            string `check:"required"`
	LogPattern          string `check:"required"`
	UseLogSystem        string
	LogRotationTime     time.Duration
	LogBackups          uint
	TextFormatter       logrus.Formatter
	UseTracer           string
	TracerAddr          string
	DisableAsyncWrite   bool   //true表示同步写日志
	LogQueueSize        int    //默认5000
	DisableReportCaller string //禁止输出日志所在方法及文件，有利于提高性能
}

/**
 * 初始化日志输出规则
 */
func InitLog(conf *LoggerConfig) error {
	err := check(conf)
	if err != nil {
		return err
	}
	log := logrus.New()
	var ft logrus.Formatter
	fmt.Printf("conf.LogFormat %v\n", conf.LogFormat)
	if conf.LogFormat == "json" {
		ft = &logrus.JSONFormatter{TimestampFormat: common.TimeFormatUtilMs}
	} else if conf.TextFormatter != nil {
		ft = conf.TextFormatter
	} else {
		//ft = &logrus.TextFormatter{DisableColors:false, TimestampFormat:"2006-01-02 15:04:05.000"}
		//ft = &nested.Formatter{HideKeys:false, TimestampFormat:"2006-01-02 15:04:05.000"}

		if conf.UseLogSystem == "true" {
			fmt.Printf("conf.LogFormat MyNestedFormatter\n")
			var f MyNestedFormatter
			f.TimestampFormat = common.TimeFormatUtilMs
			f.ShowFullLevel = true
			f.HideKeys = false
			f.NoColors = true
			ft = &f
		} else {
			var f nested.Formatter
			f.TimestampFormat = common.TimeFormatUtilMs
			f.HideKeys = false
			f.ShowFullLevel = true
			f.NoColors = true
			ft = &f
		}
	}
	ft = &MyFormatter{
		conf:      conf,
		formatter: ft,
	}
	var disableReportCaller bool
	if conf.DisableReportCaller == "true" {
		disableReportCaller = true
	}
	log.SetReportCaller(!disableReportCaller)
	log.SetFormatter(ft)
	if conf.Disable == "true" {
		log.SetOutput(ioutil.Discard)
	} else if conf.DisableAsyncWrite {
		log.SetOutput(os.Stdout)
	} else {
		size := 5000
		if conf.LogQueueSize > 0 {
			size = conf.LogQueueSize
		}
		log.SetOutput(NewAsyncWriter(size, os.Stdout))
	}
	level, _ := logrus.ParseLevel(conf.LogLevel)
	log.SetLevel(level)
	if conf.LogOnlyStout == "false" && conf.Disable != "true" {
		hook, err := newLfsHook(log, conf, ft)
		if err != nil {
			return err
		}
		log.AddHook(hook)
	}

	if conf.UseTracer != "true" {
		Logger = log
		closeTracer()
		return nil
	}
	if conf.TracerAddr == "" {
		host := os.Getenv("HOSTIP")
		if host != "" {
			conf.TracerAddr = host + ":6831"
		} else {
			return errors.New("to use tracer, LogSetting.tracer_addr can't be empty, or the env of HOSTIP cannot be empty")
		}
	}
	Logger = log
	log.Info("jaeger agent address = ", conf.TracerAddr)
	//初始化tracer
	j, err := NewJaeger(conf.ServiceName, conf.TracerAddr)
	if err != nil {
		log.Error("failed to init tracer, err: ", err)
		return nil
	}
	closeTracer()
	Tracer = j
	return nil
}

func closeTracer() {
	if Tracer != nil {
		_ = Tracer.Close()
		Tracer = nil
		Logger.Info("close tracer")
	}
}

func check(conf *LoggerConfig) error {
	err := utils.CheckRequiredStringField("LoggerConfig", conf)
	if err != nil {
		return err
	}
	if conf.LogRotationTime.Seconds() == 0 {
		return errors.New("LoggerConfig.LogRotationTime must be not empty")
	}
	if conf.LogBackups == 0 {
		return errors.New("LoggerConfig.LogBackups must be not empty")
	}
	return nil
}

//切割日志文件
func newLfsHook(log *logrus.Logger, conf *LoggerConfig, format logrus.Formatter) (logrus.Hook, error) {
	var opts []rotatelogs.Option
	if runtime.GOOS != "windows" {
		// WithLinkName为最新的日志建立软连接，以方便随着找到当前日志文件
		opts = append(opts, rotatelogs.WithLinkName(conf.LogFilePath))
	}

	opts = append(opts, rotatelogs.WithRotationTime(conf.LogRotationTime))
	opts = append(opts, rotatelogs.WithRotationCount(conf.LogBackups))
	opts = append(opts, rotatelogs.WithClock(rotatelogs.Local))

	err := os.MkdirAll(path.Dir(conf.LogFilePath), os.ModePerm)
	if err != nil {
		return nil, err
	}
	writer, err := rotatelogs.New(
		conf.LogFilePath+conf.LogPattern, opts...,
	)

	if err != nil {
		Logger.Errorf("config local file system for logger error: %v", err)
	}

	level, err := logrus.ParseLevel(conf.LogLevel)

	if err == nil {
		log.SetLevel(level)
	} else {
		log.SetLevel(logrus.InfoLevel)
	}

	lfsHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer,
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
		logrus.PanicLevel: writer,
	}, format)

	return lfsHook, nil
}

func GetLogrusLogger(l MyLog) *logrus.Logger {
	if log, ok := l.(*logrus.Entry); ok {
		return log.Logger
	}
	if log, ok := l.(*logrus.Logger); ok {
		return log
	}
	return nil
}

//创建默认的日志配置
func CreateDefaultLoggerConfig() *LoggerConfig {
	return &LoggerConfig{
		LogFilePath:     common.DefaultLogFilePath,
		LogFormat:       "text",
		LogLevel:        "info",
		LogPattern:      ".%Y%m%d",
		LogRotationTime: time.Hour * 24,
		LogBackups:      15,
		LogOnlyStout:    "true",
		UseTracer:       "false",
	}
}

type asyncWriter struct {
	writer  io.Writer
	stop    chan bool
	logChan chan []byte
}

//创建异步writer
// channelCap：通道容量，默认为5000
// writer：默认为os.Stdout, 输出到控制台
func NewAsyncWriter(channelCap int, writer io.Writer) *asyncWriter {
	cCap := 5000
	if channelCap > 0 {
		cCap = channelCap
	}
	if writer == nil {
		writer = os.Stdout
	}
	aw := &asyncWriter{writer: writer,
		logChan: make(chan []byte, cCap),
		stop:    make(chan bool),
	}
	go aw.do()
	return aw
}

//Close
func (a *asyncWriter) Close() error {
	close(a.stop)
	return nil
}

func (a *asyncWriter) do() {
	for {
		select {
		case <-a.stop:
			return
		case log := <-a.logChan:
			_, err := a.writer.Write(log)
			if err != nil {
				fmt.Println("failed to write log, err: ", err)
			}
		}
	}
}

func (a *asyncWriter) Write(p []byte) (n int, err error) {
	//if !json.Valid(p){
	//	return len(p), errors.New("invalid json bytes")
	//}
	newP := make([]byte, len(p))
	copy(newP, p)
	a.logChan <- newP
	return len(p), nil
}

type MyFormatter struct {
	conf      *LoggerConfig
	formatter logrus.Formatter
}

func (f *MyFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if f.conf.Disable == "true" {
		return nil, nil
	}
	return f.formatter.Format(entry)
}

type MyNestedFormatter struct {
	nested.Formatter
}

//自定义格式化
func (f *MyNestedFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	levelColor := getColorByLevel(entry.Level)

	timestampFormat := f.TimestampFormat
	if timestampFormat == "" {
		timestampFormat = time.StampMilli
	}

	// output buffer
	b := &bytes.Buffer{}

	// write time
	b.WriteString(entry.Time.Format(timestampFormat))

	// write level
	level := strings.ToUpper(entry.Level.String())

	if !f.NoColors {
		_, _ = fmt.Fprintf(b, "\x1b[%dm", levelColor)
	}

	b.WriteString(" [")
	if f.ShowFullLevel {
		b.WriteString(level)
	} else {
		b.WriteString(level[:4])
	}
	b.WriteString("] ")

	//write caller
	f.writeCaller(b, entry)
	if !f.NoColors && f.NoFieldsColors {
		b.WriteString("\x1b[0m")
	}
	// write message
	if f.TrimMessages {
		b.WriteString(strings.TrimSpace(entry.Message))
	} else {
		b.WriteString(entry.Message)
	}
	if !f.NoColors && !f.NoFieldsColors {
		b.WriteString("\x1b[0m")
	}
	// write fields
	if f.FieldsOrder == nil {
		f.writeFields(b, entry)
	} else {
		f.writeOrderedFields(b, entry)
	}

	b.WriteByte('\n')

	return b.Bytes(), nil
}

func (f *MyNestedFormatter) writeCaller(b *bytes.Buffer, entry *logrus.Entry) {
	if entry.HasCaller() {
		if f.CustomCallerFormatter != nil {
			_, _ = fmt.Fprintf(b, f.CustomCallerFormatter(entry.Caller))
		} else {
			_, _ = fmt.Fprintf(
				b,
				"[%s] [%s:%d] ",
				entry.Caller.Function,
				entry.Caller.File,
				entry.Caller.Line,
			)
		}
	}
}

func (f *MyNestedFormatter) writeFields(b *bytes.Buffer, entry *logrus.Entry) {
	if len(entry.Data) != 0 {
		fields := make([]string, 0, len(entry.Data))
		for field := range entry.Data {
			fields = append(fields, field)
		}

		sort.Strings(fields)

		for _, field := range fields {
			f.writeField(b, entry, field)
		}
	}
}

func (f *MyNestedFormatter) writeOrderedFields(b *bytes.Buffer, entry *logrus.Entry) {
	length := len(entry.Data)
	foundFieldsMap := map[string]bool{}
	for _, field := range f.FieldsOrder {
		if _, ok := entry.Data[field]; ok {
			foundFieldsMap[field] = true
			length--
			f.writeField(b, entry, field)
		}
	}

	if length > 0 {
		notFoundFields := make([]string, 0, length)
		for field := range entry.Data {
			if foundFieldsMap[field] == false {
				notFoundFields = append(notFoundFields, field)
			}
		}

		sort.Strings(notFoundFields)

		for _, field := range notFoundFields {
			f.writeField(b, entry, field)
		}
	}
}

func (f *MyNestedFormatter) writeField(b *bytes.Buffer, entry *logrus.Entry, field string) {
	if f.HideKeys {
		_, _ = fmt.Fprintf(b, " [%v] ", entry.Data[field])
	} else {
		_, _ = fmt.Fprintf(b, " [%s:%v] ", field, entry.Data[field])
	}
}

const (
	colorRed    = 31
	colorYellow = 33
	colorBlue   = 36
	colorGray   = 37
)

func getColorByLevel(level logrus.Level) int {
	switch level {
	case logrus.DebugLevel:
		return colorGray
	case logrus.WarnLevel:
		return colorYellow
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		return colorRed
	default:
		return colorBlue
	}
}

//让logrus.Logger实现该接口
type MyLog interface {
	WithField(key string, value interface{}) *logrus.Entry
	WithFields(fields logrus.Fields) *logrus.Entry
	WithError(err error) *logrus.Entry

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Printf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})

	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})
	Warning(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})

	Debugln(args ...interface{})
	Infoln(args ...interface{})
	Println(args ...interface{})
	Warnln(args ...interface{})
	Warningln(args ...interface{})
	Errorln(args ...interface{})
	Fatalln(args ...interface{})
	Panicln(args ...interface{})
}

//在指定tracer作用范围内，进行日志记录
func WithTracerSpan(span *JaegerSpan) MyLog {
	if span == nil {
		return Logger
	}
	return span.Logger()
}
