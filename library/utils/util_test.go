package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"library_Test/common"
)

func TestCommonEncryptPassword(t *testing.T) {
	str := CommonEncrypt("h7maJWQ5Fcm3E27S")
	fmt.Println(str)
}

func TestCommonDecryptPassword(t *testing.T) {
	str := CommonEncrypt("IUR9IvIYEz1I5jTB")
	fmt.Println("EncryptResult: ", str)
	pwd, err := CommonDecrypt(str)
	assert.NoError(t, err)
	fmt.Println("DecryptResult: ", pwd)
}

type MyStruct struct {
	A string `check:"required"`
	B int
	C bool
	D float32
	E time.Time
	F time.Duration
	G NextStruct
	H Custom `check:"required"`
}
type Custom string
type NextStruct struct {
}

func TestCheckRequiredStringField(t *testing.T) {
	target := &MyStruct{A: "a"}
	err := CheckRequiredStringField("MyStruct", target)
	assert.NoError(t, err)
}

//func TestEncryptDESECB(t *testing.T) {
//	type args struct {
//		origin string
//		key    string
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    string
//		wantErr bool
//	}{
//		{"测试模仿前端加密密码", args{
//			origin: "n%0*jkz%IxWeUXhp",
//			key:    common.DesSecretKey,
//		}, "YewLfEiTuSiWU4cLUmEN40TknnU3KPfO", false},
//		{"测试模仿前端加密密码2", args{
//			origin: "PnzD3KhhtzhuTbzP",
//			key:    common.DesSecretKey,
//		}, "jrv13jSdVX7fZaxI8gst7UTknnU3KPfO", false},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := EncryptDESECB(tt.args.origin, tt.args.key)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("EncryptDESECB() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			fmt.Println(got)
//			if got != tt.want {
//				t.Errorf("EncryptDESECB() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestEncryptAES(t *testing.T) {
	type args struct {
		origin string
		key    string
		iv     string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"测试模拟前端aes加密", args{
			origin: "PnzD3KhhtzhuTbzP",
			key:    common.AES_PASSWORD_KEY_UNI,
			iv:     common.AES_ENCRYPT_IV,
		}, "+3Ohf7a652nkevSQ+ZxKGb4rP+QqS7jErFxFHLKuPEY=", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EncryptAES(tt.args.origin, tt.args.key, tt.args.iv)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncryptAES() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("EncryptAES() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecryptAES(t *testing.T) {
	type args struct {
		origin string
		key    string
		iv     string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"测试aes解密", args{
			origin: "+3Ohf7a652nkevSQ+ZxKGb4rP+QqS7jErFxFHLKuPEY=",
			key:    common.AES_PASSWORD_KEY_UNI,
			iv:     common.AES_ENCRYPT_IV,
		}, "PnzD3KhhtzhuTbzP", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecryptAES(tt.args.origin, tt.args.key, tt.args.iv)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecryptAES() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DecryptAES() got = %v, want %v", got, tt.want)
			}
		})
	}
}