package log

import (
	"context"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	uuid "github.com/satori/go.uuid"
	"github.com/smallnest/rpcx/share"
	"github.com/uber/jaeger-client-go"
	"io"
	"library_Test/common"
	"net/http"
)

var tracer *Jaeger

//追踪器
type Jaeger struct {
	ServiceName   string
	Addr          string
	MaxPacketSize int
	Tracer        opentracing.Tracer
	Closer        io.Closer
}

//自定义日志器
type RemoteReporterLogger struct {
}

func (l *RemoteReporterLogger) Error(msg string) {
	if Logger == nil {
		fmt.Printf("RemoteReporter error: ", msg)
	}
	Logger.WithField("JaegerReporter", "RemoteReporter").Error(msg)
}

func (l *RemoteReporterLogger) Infof(msg string, args ...interface{}) {
	if Logger == nil {
		fmt.Printf(msg, args...)
		return
	}
	Logger.WithField("JaegerReporter", "RemoteReporter").Infof(msg, args...)
}

func (j *Jaeger) Close() error {
	return j.Closer.Close()
}

func (j *Jaeger) SetGlobalTracer() {
	opentracing.SetGlobalTracer(j.Tracer)
}

//获取Jaeger Tracer
func GetTracer() *Jaeger {
	return tracer
}

//tracer是否启用
func TracerIsEnabled() bool {
	return tracer != nil
}

//创建Jaeger
//输入：
// serviceName：当前服务名称
// addr: jaeger agent地址
func NewJaeger(serviceName, tracerAddr string, options ...JaegerOption) (*Jaeger, error) {
	if serviceName == "" {
		return nil, errors.New("jaeger creation failed because 'serviceName' was empty")
	}
	if tracerAddr == "" {
		return nil, errors.New("jaeger creation failed because 'tracerAddr' was empty")
	}
	opts := &JaegerOptions{}
	if len(options) != 0 {
		for _, opt := range options {
			opt(opts)
		}
	}
	j := &Jaeger{Addr: tracerAddr, ServiceName: serviceName}
	if opts.MaxPacketSize != 0 {
		j.MaxPacketSize = opts.MaxPacketSize
	}
	//MaxPacketSize默认65000
	tsp, err := jaeger.NewUDPTransport(tracerAddr, j.MaxPacketSize)
	if err != nil {
		fmt.Println("failed to create UDPTransport", err)
		return nil, err
	}
	reporterQueueSize := 10000
	if opts.ReporterQueueSize != 0 {
		reporterQueueSize = opts.ReporterQueueSize
	}
	ros := jaeger.ReporterOptions
	t, c := jaeger.NewTracer(j.ServiceName,
		jaeger.NewConstSampler(true),
		jaeger.NewRemoteReporter(tsp, ros.QueueSize(reporterQueueSize),
			ros.Logger(&RemoteReporterLogger{})),
		opts.TracerOptions...)
	j.Tracer = t
	j.Closer = c
	j.SetGlobalTracer()
	tracer = j
	return j, nil
}

type JaegerOptions struct {
	ReporterQueueSize int
	MaxPacketSize     int
	TracerOptions     []jaeger.TracerOption
}

type JaegerOption func(opts *JaegerOptions)

func SetLogger(l jaeger.Logger) JaegerOption {
	return func(opts *JaegerOptions) {
		opts.TracerOptions = append(opts.TracerOptions, jaeger.TracerOptions.Logger(l))
	}
}

func MaxPacketSize(size int) JaegerOption {
	return func(opts *JaegerOptions) {
		opts.MaxPacketSize = size
	}
}

func ReporterQueueSize(size int) JaegerOption {
	return func(opts *JaegerOptions) {
		opts.ReporterQueueSize = size
	}
}

type JaegerLogger struct {
	Logger MyLog
}

func (j *JaegerLogger) Error(msg string) {
	j.Logger.WithField("Tracer", "Jaeger").Error(msg)
}

func (j *JaegerLogger) Infof(msg string, args ...interface{}) {
	j.Logger.WithField("Tracer", "Jaeger").Infof(msg, args...)
}

type JaegerSpan struct {
	opentracing.Span
	logId string //当tracer没启用时，该值不为空
}

//创建JaegerSpan，如果span为nil,则返回nil
func NewJaegerSpan(span opentracing.Span, logId string) *JaegerSpan {
	if logId == "" {
		logId = GenerateLogId()
	}
	if span == nil {
		return &JaegerSpan{logId: logId}
	}
	return &JaegerSpan{Span: span, logId: logId}
}

//获取日志对象，在指定tracer作用范围内，进行日志记录
func (span *JaegerSpan) Logger() MyLog {
	if !TracerIsEnabled() || span.Span == nil {
		return Logger.WithField("LogId", span.logId)
	}
	ctx := span.Context().(jaeger.SpanContext)
	return Logger.WithField("Tracer", "Jaeger").
		WithField("TraceID", ctx.TraceID().String()).
		WithField("SpanID", ctx.SpanID().String()).
		WithField("LogId", span.logId)
}

//获取日志id
func (span *JaegerSpan) GetLogId() string {
	return span.logId
}

func (span *JaegerSpan) Finish() {
	if span.Span == nil {
		return
	}
	span.Span.Finish()
}

//开启一个tracer span
//	如果未启用tracer，则返回nil,nil
//输出：
// tracer span
// span的finish函数
func StartSpan(operationName string) *JaegerSpan {
	if !TracerIsEnabled() {
		return NewJaegerSpan(nil, GenerateLogId())
	}
	return startSpan(operationName)
}

func startSpan(operationName string) *JaegerSpan {
	s := tracer.Tracer.StartSpan(operationName)
	js := NewJaegerSpan(s, GenerateLogId())
	return js
}

//开启一个tracer span,并设置父级span,如果父级span为nil，则开启一个新的span
//	如果未启用tracer，则返回nil,nil
//输出：
// tracer span
// span的finish函数
func StartSpanWithParent(operationName string, parent *JaegerSpan) *JaegerSpan {
	if parent == nil {
		return StartSpan(operationName)
	}

	if !TracerIsEnabled() {
		return NewJaegerSpan(nil, parent.GetLogId())
	}
	s := parent.Span
	if s != nil {
		s = tracer.Tracer.StartSpan(operationName, opentracing.ChildOf(parent.Context()))
	}
	return NewJaegerSpan(s, parent.GetLogId())
}

//开启一个tracer span,从context中提取父级span,如果父级span为nil，则开启一个新的span
//	如果未启用tracer，则返回nil,nil
//输出：
// tracer span
// span的finish函数
func StartSpanWithParentContext(operationName string, ctx context.Context) *JaegerSpan {
	if ctx == nil {
		return StartSpan(operationName)
	}
	v := ctx.Value(common.TracerContextKey)
	if v == nil {
		return StartSpan(operationName)
	}
	parent, ok := v.(*JaegerSpan)
	if !ok {
		return StartSpan(operationName)
	}

	return StartSpanWithParent(operationName, parent)
}

//开启一个tracer span,从http header提取span作为父级，
// 如果未启用tracer，则返回nil,nil
// 如果header中没有可提取的span，则开启一个新的span,返回*JaegerSpan,nil
//输出：
// tracer span
// span的finish函数
func StartSpanWithParentFromHeader(operationName string, header http.Header) (*JaegerSpan, error) {
	if header == nil {
		return StartSpan(operationName), nil
	}
	logId := getLogIdFromHeader(header)
	if !TracerIsEnabled() {
		return NewJaegerSpan(nil, logId), nil
	}
	spCtx, err := tracer.Tracer.Extract(opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(header))
	if err != nil && err != opentracing.ErrSpanContextNotFound {
		Logger.Error(err)
		return nil, err
	}
	if spCtx == nil {
		return NewJaegerSpan(nil, logId), nil
	}
	s := tracer.Tracer.StartSpan(operationName, opentracing.ChildOf(spCtx))
	return NewJaegerSpan(s, logId), nil
}

func getLogIdFromHeader(header http.Header) string {
	logId := header.Get("logid")
	if logId == "" {
		logId = GenerateLogId()
	}
	return logId
}

//开启一个tracer span，并跟随指定span,如果父级span为nil，则开启一个新的span
//	如果未启用tracer，则返回nil,nil
//输入：
// seguito:被跟随的span
//输出：
// tracer span
// span的finish函数
func StartFollowSpan(operationName string, seguito *JaegerSpan) *JaegerSpan {
	if seguito == nil {
		return StartSpan(operationName)
	}
	if !TracerIsEnabled() {
		return NewJaegerSpan(nil, seguito.GetLogId())
	}
	s := seguito.Span
	if s != nil {
		s = tracer.Tracer.StartSpan(operationName, opentracing.FollowsFrom(seguito.Context()))
	}
	return NewJaegerSpan(s, seguito.GetLogId())
}

//从rpcx context中提取tracer span,如果提取不到，则返回nil
func GetSpanFromRpcxCtx(ctx context.Context) *JaegerSpan {
	//if rpcxContext, ok := ctx.(*share.Context); ok {
	//	span1 := rpcxContext.Value(share.OpentracingSpanServerKey)
	//	if span1 != nil {
	//		return NewJaegerSpan(span1.(*jaeger.Span), getLogIdFromCtx(rpcxContext))
	//	}
	//	return NewJaegerSpan(nil, getLogIdFromCtx(rpcxContext))
	//}
	return nil
}

func getLogIdFromCtx(ctx *share.Context) string {
	if metaData := ctx.Value(share.ReqMetaDataKey); metaData == nil {
		return ""
	} else if meta, ok := metaData.(map[string]string); !ok {
		return ""
	} else if id, ok := meta["logid"]; !ok {
		return ""
	} else {
		return id
	}
}

//从rpcx context中提取tracer span与元数据，如果提取不到，则返回nil,nil
func GetSpanAndMetadataFromRpcxCtx(ctx context.Context) (*JaegerSpan, map[string]string) {
	if ctx == nil {
		return nil, nil
	}
	rpcxContext, ok := ctx.(*share.Context)
	if !ok {
		return nil, nil
	}
	var metadata map[string]string
	reqMetadata := rpcxContext.Value(share.ReqMetaDataKey)
	if reqMetadata != nil {
		if m, ok := reqMetadata.(map[string]string); ok {
			metadata = m
		}
	}
	//span1 := rpcxContext.Value(share.OpentracingSpanServerKey)
	//span1 := rpcxContext.Value(share.OpentracingSpanServerKey)
	//if span1 != nil {
	//	return NewJaegerSpan(span1.(*jaeger.Span), getLogIdFromCtx(rpcxContext)), metadata
	//}
	return NewJaegerSpan(nil, getLogIdFromCtx(rpcxContext)), metadata
}

//将span注入到http header
func InjectSpanToHeader(span *JaegerSpan, header http.Header) error {
	if span == nil || header == nil {
		return nil
	}
	header.Add("logid", span.GetLogId())
	if span.Span == nil {
		return nil
	}
	return tracer.Tracer.Inject(span.Context(),
		opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(header))
}

//将span信息注入到map[string]string
func InjectSpanToMap(span *JaegerSpan, m map[string]string) error {
	if span == nil {
		return nil
	}
	m["logid"] = span.GetLogId()
	if span.Span == nil {
		return nil
	}
	return tracer.Tracer.Inject(span.Context(),
		opentracing.TextMap, opentracing.TextMapCarrier(m))
}

//将span信息注入到rpcx context中
//输入：
//	ctx: rpcx客户端上下文实例
//输出:
// context
// 错误
func InjectSpanToRpcxCtx(ctx context.Context, span *JaegerSpan) (context.Context, error) {
	if span == nil {
		return ctx, nil
	}
	m := map[string]string{
		"logid": span.GetLogId(),
	}
	rpcxContext, ok := ctx.(*share.Context)
	if ok {
		rpcxContext.SetValue(share.ReqMetaDataKey, m)
	}
	if span.Span != nil {
		err := tracer.Tracer.Inject(span.Context(),
			opentracing.TextMap, opentracing.TextMapCarrier(m))
		if err != nil {
			return ctx, err
		}
	}
	if ok {
		return ctx, nil
	}
	newCtx := context.WithValue(ctx, share.ReqMetaDataKey, m)
	return newCtx, nil
}

//将span信息与一些元数据注入到rpcx context中
//输入：
//	meta:元数据
//输出:
// rpc context
// 错误
func InjectSpanAndMetadataToRpcxCtx(ctx context.Context, span *JaegerSpan, meta map[string]string) error {
	if span == nil {
		return nil
	}
	meta["logid"] = span.GetLogId()
	rpcxContext, ok := ctx.(*share.Context)
	if ok {
		rpcxContext.SetValue(share.ReqMetaDataKey, meta)
	}
	if span.Span != nil {
		err := tracer.Tracer.Inject(span.Context(),
			opentracing.TextMap, opentracing.TextMapCarrier(meta))
		if err != nil {
			return err
		}
	}
	if ok {
		return nil
	}
	ctx = context.WithValue(ctx, share.ReqMetaDataKey, meta)
	return nil
}

//将JaegerSpan注入到当前context
//输入：
//	ctx: 当前上下文
//	span: 要注入到上下文的JaegerSpan
func InjectJaegerSpan(ctx context.Context, span *JaegerSpan) context.Context {
	if span == nil {
		return ctx
	}
	return context.WithValue(ctx, common.TracerContextKey, span)
}

//完成JaegerSpan
func FinishJaegerSpan(span *JaegerSpan) {
	if span.Span != nil {
		span.Finish()
	}
}

func GenerateLogId() string {
	return uuid.NewV4().String()
}
