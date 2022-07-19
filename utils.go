/**
 *@Author: pangj
 *@Description:
 *@File: utils
 *@Version:
 *@Date: 2022/07/19/16:57
 */

package hello

import "time"

func Today() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
