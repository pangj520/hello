package sql

import (
	"errors"
	"net"
	"library_Test/common"
)
import "github.com/go-sql-driver/mysql"
import "github.com/jinzhu/gorm"

var (
	//服务发现未发现任何sql实例
	ErrNoServiceAlive = errors.New("no sql service is alive")
	//无效的连接
	ErrInvalidConn = mysql.ErrInvalidConn
	//gorm相关错误
	ErrRecordNotFound       = gorm.ErrRecordNotFound
	ErrInvalidSQL           = gorm.ErrInvalidSQL
	ErrInvalidTransaction   = gorm.ErrInvalidTransaction
	ErrCantStartTransaction = gorm.ErrCantStartTransaction
	ErrUnaddressable        = gorm.ErrUnaddressable
	//mysql driver相关错误
	ErrMalformPkt        = mysql.ErrMalformPkt
	ErrNoTLS             = mysql.ErrNoTLS
	ErrCleartextPassword = mysql.ErrCleartextPassword
	ErrNativePassword    = mysql.ErrNativePassword
	ErrOldPassword       = mysql.ErrOldPassword
	ErrUnknownPlugin     = mysql.ErrUnknownPlugin
	ErrOldProtocol       = mysql.ErrOldProtocol
	ErrPktSync           = mysql.ErrPktSync
	ErrPktSyncMul        = mysql.ErrPktSyncMul
	ErrPktTooLarge       = mysql.ErrPktTooLarge
	ErrBusyBuffer        = mysql.ErrBusyBuffer
)

//判断是否需要重试
//	对于已知的错误，能够判断出是否需要重试，但未知的错误，默认进行重试
func maybeRetry(err error) bool {
	if _, ok := err.(*net.OpError); ok {
		return true
	}
	switch err {
	case ErrInvalidConn:
		return true
	case ErrNoServiceAlive:
		return false
	case ErrRecordNotFound, ErrInvalidSQL, ErrInvalidTransaction,
		ErrCantStartTransaction, ErrUnaddressable:
		return false
	case ErrMalformPkt, ErrNoTLS, ErrCleartextPassword, ErrNativePassword,
		ErrOldPassword, ErrUnknownPlugin, ErrOldProtocol, ErrPktSync, ErrPktSyncMul,
		ErrPktTooLarge, ErrBusyBuffer:
		return false
	default:
		return common.SqlRetryWithUnknownErr
	}
}
