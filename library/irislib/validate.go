package irislib

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/utils"
	"time"

	"github.com/go-playground/validator/v10"
	"k8s.io/apimachinery/pkg/api/resource"
)

var Validator *validator.Validate

var fuzzy []*regexp.Regexp = []*regexp.Regexp{
	regexp.MustCompile("^(.+)\\\\sand\\\\s(.+)|(.+)\\\\sor(.+)\\\\s$"),
	regexp.MustCompile("/(\\%27)|(\\')|(\\-\\-)|(\\%23)|(#)/ix"),
	regexp.MustCompile("/((\\%3D)|(=))[^\\n]*((\\%27)|(\\')|(\\-\\-)|(\\%3B)|(:))/i"),
	regexp.MustCompile("/\\w*((\\%27)|(\\'))((\\%6F)|o|(\\%4F))((\\%72)|r|(\\%52))/ix"),
	regexp.MustCompile("/((\\%27)|(\\'))union/ix(\\%27)|(\\')"),
}

//空的实例
type NilObject interface {
	IsNil() bool
}

func InitValidator() {
	Validator = validator.New()
	//从json标签中获取字段名
	Validator.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})

	customValidateMap := map[string]validator.Func{
		"phone":           PhoneValidate,             //检查是否为中国手机号码
		"trim":            TrimSpace,                 //对字符串进行前后去空处理
		"json":            Json,                      //检查是否为json
		"sac":             SmsAuthCode,               //检查手机验证码
		"captcha":         Captcha,                   //检查图形验证码
		"req_with":        RequiredWithFieldAndValue, //当指定的字段为某个值时，当前字段必选
		"username":        UserName,                  //对用户名进行检查
		"enname":          EnName,                    //对产品英文名进行校验
		"typename":        TypeName,                  //对变量类型
		"version":         Version,                   //对产品版本进行校验
		"req_with_parent": RequiredWithParent,        //父级不为空则该字段必选,使用时，如果后面还有其他验证，需带上omitempty
		"uint":            NonnegativeInteger,        //非负整数
		"wechat_state":    WeChatState,               //微信授权state参数
		"photo_name":      UserPhotoName,             //用户头像名
		"k8s_label":       K8sLabel,                  //k8s标签
		"k8s_name":        K8sName,                   //k8s资源名称
		"k8s_quantity":    K8sQuantity,               //k8s cpu memory数量
		"imagepath":       ImagePath,                 //镜像路径校验
		"time":            TimeValidate,              //时间验证
		"dec_pwd":         DecryptPassword,           //对加密的密码进行解密
		"fuzzy":           Fuzzy,                     //模糊匹配
		"varValue":        VarValue,                  //模糊匹配
	}
	for k := range customValidateMap {
		err := Validator.RegisterValidation(k, customValidateMap[k])
		if err != nil {
			log.Logger.Error(err)
		}
	}
}

func Fuzzy(fl validator.FieldLevel) bool {
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return false
	} else if s == "" {
		return true
	}
	for _, f := range fuzzy {
		if f.MatchString(s) {
			return false
		}
	}
	return true
}

func PhoneValidate(fl validator.FieldLevel) bool {
	time.Now().Format("2006-01-02 15:04:05")
	regular := `^(?:(?:\+|00)86)?1(?:(?:3[\d])|(?:4[5-7|9])|(?:5[0-3|5-9])|(?:6[5-7])|(?:7[0-8])|(?:8[\d])|(?:9[1|8|9]))\d{8}$`
	reg := regexp.MustCompile(regular)
	return reg.MatchString(fl.Field().String())
}

func TrimSpace(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return true
	}
	s, ok := fl.Field().Interface().(string)
	if !ok || s == "" {
		return true
	}
	if fl.Field().CanSet() {
		fl.Field().SetString(strings.TrimSpace(s))
	}
	return true
}

func Json(fl validator.FieldLevel) bool {
	switch fl.Field().Interface().(type) {
	case []interface{}:
		return true
	case map[string]interface{}:
		return true
	case string:
		break
	default:
		return false
	}
	return json.Valid(utils.Str2bytes(fl.Field().String()))
}

func SmsAuthCode(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	//对验证码进行检验
	regular := fmt.Sprintf("^[0-9]{%d}$", common.AuthCodeLen)
	reg := regexp.MustCompile(regular)
	return reg.MatchString(s)
}

func Captcha(fl validator.FieldLevel) bool {
	s := fl.Field().String()
	regular := fmt.Sprintf("^[0-9]{%d}$", common.CaptchaLength)
	reg := regexp.MustCompile(regular)
	return reg.MatchString(s)
}

//检查用户名，由3-16个数字、字母、下划线组成，不允许中文与特殊字符
func UserName(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	regular := fmt.Sprintf("^[0-9a-zA-Z_]{%d,%d}$", common.UserNameMinLen, common.UserNameMaxLen)
	reg := regexp.MustCompile(regular)
	return reg.MatchString(s)
}

//检查产品英文名，由字母组成，不允许中文、数字、横杠与特殊字符等
func EnName(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	regular := fmt.Sprintf("^[a-zA-Z][0-9a-zA-Z]{0,299}$")
	reg := regexp.MustCompile(regular)
	return reg.MatchString(s)
}

//配置变量值校验，不允许含有单引号、双引号、反斜线
func VarValue(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	if strings.Contains(s, "'") || strings.Contains(s, "/") || strings.Contains(s, "\"") {
		return false
	}
	return true
}

//只允许字母、数字及下划线组合, 不能数字开头,长度不大于30
func TypeName(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	regular := fmt.Sprintf("^[a-zA-Z_][0-9a-zA-Z_]{0,29}$")
	reg := regexp.MustCompile(regular)
	return reg.MatchString(s)
}

//检查产品英文名，由字母、下划线，横杠组成，不允许中文与特殊字符
func Version(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	versionRegexpCon := `(v|V)([0-9]+(\.[0-9]+){0,2})`
	regular := fmt.Sprintf("^" + versionRegexpCon + "$")
	reg := regexp.MustCompile(regular)
	return reg.MatchString(s)
}

//父级不为nil,此字段必选，使用时，如果后面还有其他验证，后面需带上omitempty
func RequiredWithParent(fl validator.FieldLevel) bool {
	var p reflect.Value
	if fl.Parent().Kind() == reflect.Ptr {
		p = fl.Parent().Elem()
	} else {
		p = fl.Parent()
	}
	pHasValue := checkFieldValue(fl, p)
	if pHasValue {
		return hasValue(fl)
	}
	return true
}

//用法：validate:"req_with={FIELD}:{FIELD_PARAM}"
func RequiredWithFieldAndValue(fl validator.FieldLevel) bool {
	param := strings.Split(fl.Param(), `:`)
	paramField := param[0]
	paramValue := param[1]

	if paramField == "" {
		return true
	}

	// param field reflect.Value.
	var paramFieldValue reflect.Value

	if fl.Parent().Kind() == reflect.Ptr {
		paramFieldValue = fl.Parent().Elem().FieldByName(paramField)
	} else {
		paramFieldValue = fl.Parent().FieldByName(paramField)
	}

	if isEq(paramFieldValue, paramValue) == false {
		return true
	}
	return hasValue(fl)
}

func hasValue(fl validator.FieldLevel) bool {
	return requireCheckFieldKind(fl, "")
}

func requireCheckFieldKind(fl validator.FieldLevel, param string) bool {
	field := fl.Field()
	if len(param) > 0 {
		if fl.Parent().Kind() == reflect.Ptr {
			field = fl.Parent().Elem().FieldByName(param)
		} else {
			field = fl.Parent().FieldByName(param)
		}
	}
	return checkFieldValue(fl, field)
}

func checkFieldValue(fl validator.FieldLevel, field reflect.Value) (res bool) {
	switch field.Kind() {
	case reflect.Slice, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Chan, reflect.Func:
		return !field.IsNil()
	default:
		_, _, nullable := fl.ExtractType(field)
		if nullable && field.Interface() != nil {
			return true
		}
		if !field.IsValid() {
			return false
		}
		defer func() {
			err := recover()
			if err != nil {
				log.Logger.Error(err)
				if !strings.Contains(err.(error).Error(), "comparing uncomparable type") {
					res = false
					return
				}
				//出现不可比较的情况，那么该参数必须实现接口NilObject
				if _, ok := field.Interface().(NilObject); !ok {
					log.Logger.Info(field.Type().String() + " does not implement NilObject")
					res = false
					return
				}
				m := field.MethodByName("IsNil")
				vs := m.Call([]reflect.Value{})
				res = !vs[0].Bool()
				return
			}
		}()
		return field.Interface() != reflect.Zero(field.Type()).Interface()
	}
}

func isEq(field reflect.Value, value string) bool {
	switch field.Kind() {

	case reflect.String:
		return field.String() == value

	case reflect.Slice, reflect.Map, reflect.Array:
		p := asInt(value)

		return int64(field.Len()) == p

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		p := asInt(value)

		return field.Int() == p

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		p := asUint(value)

		return field.Uint() == p

	case reflect.Float32, reflect.Float64:
		p := asFloat(value)

		return field.Float() == p
	case reflect.Bool:
		p := asBool(value)

		return field.Bool() == p
	}

	panic(fmt.Sprintf("Bad field type %T", field.Interface()))
}

func asBool(param string) bool {

	b, err := strconv.ParseBool(param)
	panicIf(err)

	return b
}

func asInt(param string) int64 {

	i, err := strconv.ParseInt(param, 0, 64)
	panicIf(err)

	return i
}

func asUint(param string) uint64 {

	i, err := strconv.ParseUint(param, 0, 64)
	panicIf(err)

	return i
}

func asFloat(param string) float64 {

	i, err := strconv.ParseFloat(param, 64)
	panicIf(err)

	return i
}

func panicIf(err error) {
	if err != nil {
		panic(err.Error())
	}
}

//检查是否为非负整数
func NonnegativeInteger(fl validator.FieldLevel) bool {
	switch fl.Field().Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return regexp.MustCompile("^[0-9]+$").MatchString(fl.Field().String())
	}
}

func WeChatState(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	rex := "^[0-9a-zA-Z]{" + strconv.Itoa(common.WeChatAuthStateLen) + "}$"
	return regexp.MustCompile(rex).MatchString(s)
}

func UserPhotoName(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	rex := "^[0-9a-zA-Z]{" + strconv.Itoa(common.UserPhotoNameLen) + "}$"
	return regexp.MustCompile(rex).MatchString(s)
}

//k8s标签
func K8sLabel(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	if !strings.Contains(s, ":") {
		return false
	}
	kv := strings.Split(s, ":")
	nameRex := "^([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$"
	if !regexp.MustCompile(nameRex).MatchString(kv[0]) {
		return false
	}
	valeRex := "^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$"
	return regexp.MustCompile(valeRex).MatchString(kv[1])
}

//k8s资源名称
func K8sName(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	rex := `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	return regexp.MustCompile(rex).MatchString(s)
}

//镜像路径
func ImagePath(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	rex := `^[a-z0-9]([a-z0-9._/-]*[a-z0-9])?(:[a-zA-Z0-9_]([a-z0-9._-]{0,126}[a-zA-Z0-9_])?)?$`
	return regexp.MustCompile(rex).MatchString(s)
}

//k8s cpu memory值
func K8sQuantity(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	_, err := resource.ParseQuantity(s)
	if err != nil {
		log.Logger.Info(err)
		return false
	}
	return true
}

//时间字符串验证
func TimeValidate(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	timeFmt := "2006-01-02 15:04:05"
	format := fl.Param()
	if format != "" {
		timeFmt = format
	}
	_, err := time.Parse(timeFmt, s)
	if err != nil {
		log.Logger.Info(err)
		return false
	}
	return true
}

//对加密密码进行解密
func DecryptPassword(fl validator.FieldLevel) bool {
	if !fl.Field().IsValid() {
		return false
	}
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return true
	}
	if s == "" {
		return false
	}
	pwd, err := utils.DecryptAES(s, common.AES_PASSWORD_KEY_UNI, common.AES_ENCRYPT_IV)
	if err != nil {
		log.Logger.Info("unable to decrypt password, ", err)
		return false
	}
	fl.Field().SetString(pwd)
	return true
}
