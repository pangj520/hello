package irislib

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"library_Test/common"
	"library_Test/common/retcode"
	"library_Test/library/log"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/kataras/iris/v12"
	"github.com/tidwall/gjson"
)

var (
	FlagIp = "ip"
)

type ParamPosition string

const (
	Body        ParamPosition = "body"        //参数位置在请求体
	Path        ParamPosition = "path"        //参数位置在URL上，格式如：/get/1
	QueryString ParamPosition = "querystring" //参数在URL上，格式如：?a=1&b=2
	Header      ParamPosition = "header"      //参数在hetp header上
	Nil         ParamPosition = ""            //无
)

func (p ParamPosition) String() string {
	return string(p)
}

type OptResultAndMsg struct {
	OptResult uint16
	Msg       string
}

type IrisResponse struct {
	OptResultAndMsg
	Obj interface{}
}

func (r IrisResponse) MarshalJSON() ([]byte, error) {
	var isNil bool
	var bs []byte
	if r.Obj != nil {
		//检查obj是否有效
		v := reflect.ValueOf(r.Obj)
		t := reflect.TypeOf(r.Obj)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			v = v.Elem()
		}
		switch t.Kind() {
		case reflect.Slice:
			if bts, ok := v.Interface().([]byte); !ok {
				return nil, errors.New(fmt.Sprintf("obj of type %s is not supported", v.Type()))
			} else if !json.Valid(bts) {
				return nil, errors.New("obj is not a valid json []byte")
			} else {
				bs = bts
			}
		case reflect.Struct:
			if !v.IsValid() {
				isNil = true
			}
		case reflect.Interface, reflect.Map:
			isNil = v.IsNil()
		default:
			return nil, errors.New(fmt.Sprintf("obj of type %s is not supported", v.Type()))
		}
	} else {
		isNil = true
	}
	if isNil {
		str := fmt.Sprintf(`{"%s":%d`, common.OptResult, r.OptResult)
		if r.Msg != "" {
			str = fmt.Sprintf(`%s,"%s":"%s"}`, str, common.Message, r.Msg)
		} else {
			str += "}"
		}
		return []byte(str), nil
	}

	var err error
	if len(bs) == 0 {
		bs, err = json.Marshal(r.Obj)
	}
	if err != nil {
		return nil, err
	}
	//将optResult与msg放入[]byte中
	body := bs[:len(bs)-1]
	addStr := fmt.Sprintf(`"%s":%d`, common.OptResult, r.OptResult)
	if len(body) != 1 {
		addStr = "," + addStr
	}
	add := []byte(addStr)
	if r.Msg != "" {
		add = append(add, []byte(fmt.Sprintf(`,"%s":"%s"`, common.Message, r.Msg))...)
	}
	add = append(add, bs[len(bs)-1])
	body = append(body, add...)
	return body, nil
}

func (r *IrisResponse) UnmarshalJSON(b []byte) error {
	rs := gjson.GetManyBytes(b, common.OptResult, common.Message)
	if rs[0].Exists() {
		r.OptResult = uint16(rs[0].Uint())
	}
	if rs[1].Exists() {
		r.Msg = rs[1].Str
	}
	if r.Obj == nil {
		return nil
	}
	ot := reflect.TypeOf(r.Obj)
	if ot.Kind() != reflect.Ptr && ot.Kind() != reflect.Map {
		return errors.New("obj must be a pointer or map")
	}
	err := json.Unmarshal(b, r.Obj)
	return err
}

type ResponseCallback func(ctx iris.Context, statusCode int, opt uint16, resp *IrisResponse)

//记录请求信息，绑定请求参数，校验请求参数,不支持文件上传
// position: 参数位置,为空则表示无请求参数
// validateStruct: 绑定请求参数的结构体,为空表示不进行校验
func LogBindAndValidate(position ParamPosition, validateStruct interface{}) iris.Handler {
	return logBindAndValidateWithPage(position, validateStruct, false)
}

// LogBindAndValidateWithPage 记录请求信息，绑定请求参数，校验请求参数,不支持文件上传 pageInt=true时，把page
func logBindAndValidateWithPage(position ParamPosition, validateStruct interface{}, pageInt bool) iris.Handler {
	return func(ctx iris.Context) {
		s := log.StartSpanWithParent("http.LogBindAndValidate", GetJaegerSpan(ctx))
		defer log.FinishJaegerSpan(s)

		start := time.Now()
		path := ctx.Request().RequestURI
		method := ctx.Method()
		ip := GetRealIp(ctx)
		p, err := url.QueryUnescape(path)
		if err != nil {
			log.Logger.Info(err)
			p = path
		}
		defer responseTime(ctx, start, p)()

		if position == Nil {
			//无请求参数，记录请求信息
			log.WithTracerSpan(s).Info(ip, " ", method, " ", p, " => {}")
			ctx.Values().Set(common.KeyJaegerSpan, s)
			ctx.Next()
			return
		}

		bs, err := getParamsByPosition(ctx, position, pageInt)
		if err != nil {
			return
		}

		//记录请求信息
		log.WithTracerSpan(s).Info(ip, " ", method, " ", p, " => ", string(bs))

		ctx.Values().Set(common.KeyOriginalParam, bs)

		if validateStruct == nil {
			//不校验请求参数
			ctx.Next()
			return
		}

		//绑定请求参数并校验
		t := reflect.TypeOf(validateStruct)
		if t.Kind() == reflect.Ptr {
			v := reflect.ValueOf(validateStruct).Elem()
			t = v.Type()
			validateStruct = v.Interface()
		}
		ptr := reflect.New(t).Interface()
		err = json.Unmarshal(bs, ptr)
		if err != nil {
			log.WithTracerSpan(s).Error(err)
			if e, ok := err.(*json.UnmarshalTypeError); ok {
				ErrorResponse(ctx, iris.StatusBadRequest, retcode.SYSTEN_INTL_PARAM_INVALID, e.Field+common.MsgFailValidateSuffix)
				return
			}
			ErrorResponse(ctx, iris.StatusBadRequest, retcode.SYSTEN_INTL_PARAM_INVALID, common.MsgFailValidateSuffix)
			return
		}
		err = Validator.Struct(ptr)
		if err != nil {
			log.WithTracerSpan(s).Warnf("validate field failed. %s", err.Error())
			st, msg := ParseErrorMsg(err)
			ErrorResponse(ctx, st, retcode.SYSTEN_INTL_PARAM_INVALID, msg)
			return
		}

		ctx.Values().Set(common.KeyValidationParam, ptr)
		ctx.Values().Set(common.KeyJaegerSpan, s)
		ctx.Next()
	}
}

//响应耗时
func responseTime(ctx iris.Context, s time.Time, path string) func() {
	return func() {
		span := GetJaegerSpan(ctx)
		log.WithTracerSpan(span).Info(path, " => ", time.Since(s).String())
	}
}

func GetRealIp(ctx iris.Context) string {
	v := ctx.Values().Get(FlagIp)
	if v != nil {
		return v.(string)
	}
	var ip string
	headers := []string{
		ctx.GetHeader("X-Real-IP"), ctx.GetHeader("X-real-ip"), ctx.GetHeader("X-Forwarded-For"),
		ctx.GetHeader("Proxy-Client-IP"), ctx.GetHeader("WL-Proxy-Client-IP"),
		ctx.GetHeader("HTTP_CLIENT_IP"), ctx.GetHeader("HTTP_X_FORWARDED_FOR"),
		ctx.Request().RemoteAddr,
	}
	for _, h := range headers {
		for _, s := range strings.Split(h, ",") {
			if strings.ToLower(s) == "unknown" {
				continue
			}
			ip = s
			break
		}
		if ip != "" {
			break
		}
	}
	ip = strings.TrimSpace(ip)
	ctx.Values().Set(FlagIp, ip)
	return ip
}

func getParamsByPosition(ctx iris.Context, position ParamPosition, pageInt bool) ([]byte, error) {
	pm := make(map[string]interface{})
	switch position {
	case Path:
		if ctx.Params() == nil {
			break
		}
		for _, v := range ctx.Params().Store {
			pm[v.Key] = v.Value()
		}
	case QueryString:
		if ctx.URLParams() == nil {
			break
		}
		for k := range ctx.URLParams() {
			//pm[k] = ctx.URLParams()[k]
			if pageInt && (k == "pageNum" || k == "pageDataCount") {
				pm[k], _ = strconv.Atoi(ctx.URLParam(k))
			} else {
				pm[k] = ctx.URLParams()[k]
			}
		}
	case Body:
		err := ctx.ReadJSON(&pm)
		if err != nil {
			log.Logger.Error(err)
			ErrorResponse(ctx, iris.StatusBadRequest, retcode.SYSTEN_INTL_PARAM_INVALID, common.MsgParseParamFail)
			return nil, err
		}
	}

	bs, err := json.Marshal(&pm)
	if err != nil {
		log.Logger.Error(err)
		ErrorResponse(ctx, iris.StatusInternalServerError, retcode.SYSTEM_INTL_ERROR, "")
		return nil, err
	}
	return bs, nil
}

func ParseErrorMsg(err error) (int, string) {
	var msg string
	switch err.(type) {
	case *validator.InvalidValidationError:
		return iris.StatusInternalServerError, ""
	case validator.ValidationErrors:
		es := err.(validator.ValidationErrors)
		for _, item := range es {
			msg += fmt.Sprintf("%s%s;", item.Field(), common.MsgFailValidateSuffix)
		}
		return iris.StatusBadRequest, msg
	default:
		return iris.StatusInternalServerError, ""
	}
}

//获取校验通过的参数封装的struct
func GetValidationParam(ctx iris.Context) interface{} {
	return ctx.Values().Get(common.KeyValidationParam)
}

//获取原始参数数据[]byte
func GetOriginalParamForBytes(ctx iris.Context) []byte {
	return ctx.Values().Get(common.KeyOriginalParam).([]byte)
}

//将原始参数数据反序列化为指定类型
func GetOriginalParamForTarget(ctx iris.Context, target interface{}) error {
	v := ctx.Values().Get(common.KeyOriginalParam)
	if v == nil {
		return nil
	}
	bs := v.([]byte)
	return json.Unmarshal(bs, target)
}

//统一处理响应
//输入：
//	ctx: 请求上下文
//	opt: 内部返回码
//	err: 错误
//	resp: 响应参数，不需要包含oprResult与msg字段
func CommonResponse(ctx iris.Context, opt uint16, err error, resp interface{}) {
	irisResp := new(IrisResponse)
	irisResp.OptResult = opt
	irisResp.Obj = resp
	if opt != retcode.SYSTEM_INTL_OPERATION_SUCCEED {
		//根据不同的opt定义不同的msg
		irisResp.Msg = retcode.Mapper[opt]
		if irisResp.Msg == "" && err != nil {
			//没有定义msg,则使用错误的信息
			irisResp.Msg = err.Error()
		}
		Response(ctx, iris.StatusOK, opt, irisResp)
		return
	}
	SuccessResponse(ctx, irisResp)
}

// RegisterResponseCallback 注册响应回调
func RegisterResponseCallback(ctx iris.Context, cb ResponseCallback) {
	ctx.Values().Set(common.KeyIrisResponseCallback, cb)
}

// GetResponseCallback 获取当前响应回调
func GetResponseCallback(ctx iris.Context) ResponseCallback {
	v := ctx.Values().Get(common.KeyIrisResponseCallback)
	if v, ok := v.(ResponseCallback); ok {
		return v
	}
	return nil
}

//返回响应
func Response(ctx iris.Context, statusCode int, opt uint16, resp *IrisResponse) {
	s := log.StartSpanWithParent("http.response", GetJaegerSpan(ctx))
	defer log.FinishJaegerSpan(s)

	ctx.Header("Content-Type", "application/json; charset=UTF-8")
	if statusCode == 0 {
		statusCode = iris.StatusOK
	}
	ctx.StatusCode(statusCode)
	if resp == nil {
		resp = new(IrisResponse)
	}
	if resp.OptResult != opt {
		resp.OptResult = opt
	}
	if ctx.Values().Get(common.IsTransformInternalCode) != nil {
		if exCode, exMsg, err := retcode.TryGetExternalCodeAndMsg(opt); err == nil {
			resp.OptResult = exCode
			if resp.Msg == "" {
				resp.Msg = exMsg
			}
		}
	}
	bs, err := json.Marshal(resp)
	if err != nil {
		log.WithTracerSpan(s).Error(err)
	}
	//响应回调
	cb := GetResponseCallback(ctx)
	if cb != nil {
		go cb(ctx, statusCode, opt, resp)
	}
	if len(bs) < 128 {
		log.WithTracerSpan(s).Info("Response => " + string(bs))
	} else {
		log.WithTracerSpan(s).Info("Response => big data")
	}
	log.WithTracerSpan(s).Info("http stats code = ", ctx.GetStatusCode())
	_, err = ctx.JSON(resp)
	if err != nil {
		log.WithTracerSpan(s).Error(err)
	}
}

//错误返回
func ErrorResponse(ctx iris.Context, statusCode int, opt uint16, msg string) {
	resp := new(IrisResponse)
	resp.Msg = msg
	Response(ctx, statusCode, opt, resp)
}

//成功返回
func SuccessResponse(ctx iris.Context, resp *IrisResponse) {
	Response(ctx, iris.StatusOK, retcode.SYSTEM_EXTL_OPERATION_SUCCEED, resp)
}

func GetJaegerSpan(ctx iris.Context) *log.JaegerSpan {
	js := ctx.Values().Get(common.KeyJaegerSpan)
	if js == nil {
		return nil
	}
	if s, ok := js.(*log.JaegerSpan); ok {
		return s
	}
	return nil
}

//开启一个tracer span,并从iris.Context提取父级span，
// 如果从iris.Context中未找到则创建一个新的span
func StartSpanWithParent(operationName string, ctx iris.Context) *log.JaegerSpan {
	return log.StartSpanWithParent(operationName, GetJaegerSpan(ctx))
}

//开启一个tracer span，并从iris.Context提取要跟随的span，
// 如果从iris.Context中未找到则创建一个新的span
func StartFollowSpan(operationName string, ctx iris.Context) *log.JaegerSpan {
	return log.StartFollowSpan(operationName, GetJaegerSpan(ctx))
}
