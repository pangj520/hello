package utils

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"library_Test/common"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

var (
	random               = rand.New(rand.NewSource(time.Now().UnixNano()))
	NumberLetterChars    = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	ImageUserPasswordKey = []byte(common.ImageUserPasswordKey)
	CommonAesEncryptKey  = []byte(common.AesEncryptKey)
)

type Page struct {
	PageSize      int   `json:"pageSize"`      //每页记录数
	PageDataCount int   `json:"pageDataCount"` //每页记录数
	PageNum       int   `json:"pageNum"`       //当前页数
	TotalCount    int64 `json:"totalCount"`    //总记录数
	Disable       bool  `json:"-"`             //禁用分页
}

func (p *Page) GetPageSize() int {
	if p.PageSize < 1 {
		return common.PageSize
	}
	return p.PageSize
}

func (p *Page) GetPageDataCount() int {
	if p.PageDataCount < 1 {
		return common.PageSize
	}
	return p.PageDataCount
}

func (p *Page) GetPageNum() int {
	if p.PageNum < 1 {
		return common.PageNum
	}
	return p.PageNum
}

//字符串转[]byte
func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

//[]byte转string
func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

//生成一个包含数字，字母的随机字符串
func GenerateRandomStr(maxLen int, prefix string) string {
	length := maxLen - len(prefix)
	ran := len(NumberLetterChars)
	var name = make([]byte, length)
	for i := 0; i < length; i++ {
		name[i] = NumberLetterChars[random.Intn(ran)]
	}
	return prefix + string(name)
}

//字符串通用加密，只能通过CommonDecrypt()才能解密
func CommonEncrypt(oriStr string) string {
	return AesEncryptCBC([]byte(oriStr), CommonAesEncryptKey)
}

//加密的字符串进行通用解密
//输入：
// encryptStr：经过CommonEncrypt()加密后的字符串
func CommonDecrypt(encryptStr string) (string, error) {
	res, err := AesDecryptCBC(encryptStr, CommonAesEncryptKey)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

//AES加密，cbc模式
//输入:
//	origData: 要加密的数据
//	key: 秘钥
//输出:
//	aes加密结果使用base64编码结果
func AesEncryptCBC(origData []byte, key []byte) string {
	// 分组秘钥
	// NewCipher该函数限制了输入k的长度必须为16, 24或者32
	block, _ := aes.NewCipher(key)
	blockSize := block.BlockSize()                              // 获取秘钥块的长度
	origData = pkcs5Padding(origData, blockSize)                // 补全码
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize]) // 加密模式
	encrypted := make([]byte, len(origData))                    // 创建数组
	blockMode.CryptBlocks(encrypted, origData)                  // 加密
	return base64.StdEncoding.EncodeToString(encrypted)
}

//AES解密，cbc模式
//输入:
//	base64Data: 要解密的数据，base64编码
//	key: 秘钥
//输出:
//	解密结果
func AesDecryptCBC(base64Data string, key []byte) (decrypted []byte, err error) {
	encrypted, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return
	}
	block, _ := aes.NewCipher(key)                              // 分组秘钥
	blockSize := block.BlockSize()                              // 获取秘钥块的长度
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize]) // 加密模式
	decrypted = make([]byte, len(encrypted))                    // 创建数组
	blockMode.CryptBlocks(decrypted, encrypted)                 // 解密
	decrypted = pkcs5UnPadding(decrypted)                       // 去除补全码
	return
}

func pkcs5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func pkcs5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	if length-unpadding < 0 || (length-unpadding) > length {
		return nil
	}
	return origData[:(length - unpadding)]
}

//打印debug信息
//TODO　生产环境下只输出错误信息，不输出debug信息
func GetDebugStack(err error, stack []byte) string {
	return fmt.Sprintf("%s\n%s\n", err.Error(), string(stack))
}

//检查struct tag中含有check标签的值为required的类型为string的字段是否有值
//输入:
//	objName: 实例名称
//	obj: 结构体指针
func CheckRequiredStringField(objName string, obj interface{}) error {
	if obj == nil {
		return errors.New(objName + "is nil")
	}
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return errors.New(objName + " must be a pointer")
	}
	if v.IsNil() {
		return errors.New(objName + "is nil")
	}
	v = v.Elem()
	t = t.Elem()
	if v.Kind() != reflect.Struct {
		return errors.New("do not check for non-common instance")
	}
	errorStr := "%s.%s must be not empty"
	var tf reflect.StructField
	var vf reflect.Value
	for i := 0; i < v.NumField(); i++ {
		tf = t.Field(i)
		vf = v.Field(i)
		if tf.Tag.Get("check") != "required" {
			continue
		}
		if vf.Kind() != reflect.String {
			return errors.New("non-string fields are not supported")
		}
		if len(strings.TrimSpace(vf.String())) == 0 {
			return errors.New(fmt.Sprintf(errorStr, objName, tf.Name))
		}
	}
	return nil
}

///**
//DES ECB加密
//*/
//func EncryptDESECB(origin, key string) (string, error) {
//	data := []byte(origin)
//	keyByte := []byte(key)
//	block, err := des.NewCipher(keyByte)
//	if err != nil {
//		return "", nil
//	}
//	bs := block.BlockSize()
//	//对明文数据进行补码
//	data = pkcs5Padding(data, bs)
//	if len(data)%bs != 0 {
//		return "", errors.New("Need a multiple of the blocksize")
//	}
//	out := make([]byte, len(data))
//	dst := out
//	for len(data) > 0 {
//		//对明文按照blocksize进行分块加密
//		//必要时可以使用go关键字进行并行加密
//		block.Encrypt(dst, data[:bs])
//		data = data[bs:]
//		dst = dst[bs:]
//	}
//	return base64.StdEncoding.EncodeToString(out), nil
//}
//
///**
//DES ECB解密
//*/
//func DecryptDESECB(origin, key string) (decR string, err error) {
//	defer func() {
//		e := recover()
//		if er, ok := e.(error); ok {
//			err = er
//			return
//		}
//	}()
//	data, err := base64.StdEncoding.DecodeString(origin)
//	if err != nil {
//		return "", err
//	}
//	keyByte := []byte(key)
//	block, err := des.NewCipher(keyByte)
//	if err != nil {
//		return "", err
//	}
//	bs := block.BlockSize()
//	if len(data)%bs != 0 {
//		return "", errors.New("crypto/cipher: input not full blocks")
//	}
//	out := make([]byte, len(data))
//	dst := out
//	for len(data) > 0 {
//		block.Decrypt(dst, data[:bs])
//		data = data[bs:]
//		dst = dst[bs:]
//	}
//	out = pkcs5UnPadding(out)
//	return string(out), nil
//}

//从context中获取数据库事务实例
func GetDbTransactionFormCtx(ctx context.Context) *gorm.DB {
	var tx *gorm.DB
	value := ctx.Value(common.ContextKeyDbTransaction)
	if value != nil {
		if t, ok := value.(*gorm.DB); ok {
			tx = t
		}
	}
	return tx
}

/*
SHA256加密
*/
func EncryptSHA256(origin string) string {
	h := sha256.New()
	h.Write([]byte(origin))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

//如果ip为ipv6，则加上"[]"
func MaybeIpv6(ip string) string {
	if strings.Contains(ip, ":") {
		if strings.HasPrefix(ip, "[") {
			return ip
		}
		return "[" + ip + "]"
	}
	return ip
}

/**
AES加密
*/
func EncryptAES(origin, key, iv string) (string, error) {
	data := []byte(origin)
	keyBytes := []byte(key)
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return "", err
	}
	data = pkcs5Padding(data, block.BlockSize())
	offset := []byte(iv)
	blockMode := cipher.NewCBCEncrypter(block, offset)
	blockMode.CryptBlocks(data, data)
	return base64.StdEncoding.EncodeToString(data), nil
}

/**
AES解密
*/
func DecryptAES(origin, key, iv string) (out string, err error) {
	defer func() {
		e := recover()
		if er, ok := e.(error); ok {
			err = er
			return
		}
	}()
	ciphertext, err := base64.StdEncoding.DecodeString(origin)
	if err != nil {
		return "", err
	}
	keyBytes := []byte(key)
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return "", err
	}
	if len(ciphertext) < aes.BlockSize {
		return "", errors.New("密钥值过短")
	}
	offset := []byte(iv)

	blockMode := cipher.NewCBCDecrypter(block, offset)
	blockMode.CryptBlocks(ciphertext, ciphertext)
	res := pkcs5UnPadding(ciphertext)
	if res != nil {
		return string(res), nil
	}
	return "", errors.New("密文解密失败")
}
