/**
 *描述：slice处理
 *      提供slice处理的基础方法，例如向slice插入、删除等
 *作者：江洪
 *时间：2019-6-24
 */

package slice

/*
 * 在Slice指定位置插入元素
 * params:
 *   s: slice对象指针，类型为*[]interface{}
 *   index: 要插入元素的位置索引
 *   value: 要插入的元素
 * return:
 *   无
 */
func SliceInsert(s *[]interface{}, index int, value interface{}) {
	rear := append([]interface{}{}, (*s)[index:]...)
	*s = append(append((*s)[:index], value), rear...)
}

/*
 * 在Slice指定位置插入元素指针版本
 * params:
 *   s: 指针对象slice对象指针，类型为*[]*interface{}
 *   index: 要插入元素的位置索引
 *   value: 要插入的元素指针
 * return:
 *   无
 */
func SliceInsertP(s *[]*interface{}, index int, value *interface{}) {
	rear := append([]*interface{}{}, (*s)[index:]...)
	*s = append(append((*s)[:index], value), rear...)
}

/*
 * 删除Slice中指定位置元素的元素
 * params:
 *   s: slice对象指针，类型为*[]interface{}
 *   index: 要删除元素的索引
 * return:
 *   无
 * 说明：直接操作传入的Slice对象，传入的序列地址不变，但内容已经被修改
 */
func SliceRemoveByIndex(s *[]interface{}, index int) {
	*s = append((*s)[:index], (*s)[index+1:]...)
}

/*
 * 删除Slice中指定位置元素的元素指针版本
 * params:
 *   s: 指针对象slice对象指针，类型为*[]*interface{}
 *   index: 要删除元素的索引
 * return:
 *   无
 * 说明：直接操作传入的Slice对象，传入的序列地址不变，但内容已经被修改
 */
func SliceRemoveByIndexP(s *[]*interface{}, index int) {
	*s = append((*s)[:index], (*s)[index+1:]...)
}

/*
 * 清空Slice
 * params:
 *   s: slice对象指针，类型为*[]interface{}
 * return:
 *   无
 */
func SliceClear(s *[]interface{}) {
	*s = (*s)[0:0]
}

/*
 * 清空Slice指针版本
 * params:
 *   s: 指针对象slice对象指针，类型为*[]interface{}
 * return:
 *   无
 */
func SliceClearP(s *[]*interface{}) {
	*s = (*s)[0:0]
}
