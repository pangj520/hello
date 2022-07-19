package testutil

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

type Format string

const (
	FormatDefault      Format = "%v"
	FormatPlusV        Format = "%+v"
	FormatBool         Format = "%t"
	FormatString       Format = "%s"
	FormatQuotedString Format = "%q"
	FormatDecimal      Format = "%d"
	FormatFloat        Format = "%f"
)

func Wait(t time.Duration) {
	timer := time.NewTimer(t)
	<-timer.C
	timer.Stop()
}

type AssertOption func(*assertOption)

type assertOption struct {
	fmtOpts   []FormatOption
	equalFunc func(x, y interface{}) bool
}

func WithEqualFunc(equalFn func(x, y interface{}) bool) AssertOption {
	return func(opt *assertOption) { opt.equalFunc = equalFn }
}

func WithFormat(fmtOptions ...FormatOption) AssertOption {
	return func(opt *assertOption) { opt.fmtOpts = append(opt.fmtOpts, fmtOptions...) }
}

func NewAssertOption(options ...AssertOption) *assertOption {
	o := &assertOption{
		fmtOpts:   make([]FormatOption, 0, 10),
		equalFunc: reflect.DeepEqual,
	}
	for _, opt := range options {
		opt(o)
	}
	return o
}

type FormatOption func(*formatOption)

type formatOption struct {
	in        []interface{}
	out       []interface{}
	expect    []interface{}
	fmtIn     Format
	fmtOut    Format
	fmtExpect Format
}

// func WithInput(input interface{}) Option {
// 	return func(opt *AssertOption) { opt.fmtOpt.in, opt.fmtOpt.fmtIn = []interface{}{input}, FormatDefault }
// }

func WithInputFormat(format Format, args ...interface{}) FormatOption {
	return func(opt *formatOption) {
		opt.fmtIn = format
		opt.in = args
	}
}

// func WithOutput(output interface{}) Option {
// 	return func(opt *AssertOption) { opt.fmtOpt.out, opt.fmtOpt.fmtOut = []interface{}{output}, FormatDefault }
// }

func WithOutputFormat(format Format, args ...interface{}) FormatOption {
	return func(opt *formatOption) {
		opt.fmtOut = format
		opt.out = args
	}
}

// func WithExpectOutput(expect interface{}) Option {
// 	return func(opt *AssertOption) {
// 		opt.fmtOpt.expect, opt.fmtOpt.fmtExpect = []interface{}{expect}, FormatDefault
// 	}
// }

func WithExpectOutputFormat(format Format, args ...interface{}) FormatOption {
	return func(opt *formatOption) {
		opt.fmtExpect = format
		opt.expect = args
	}
}

func WithBothOutputFormat(format Format) FormatOption {
	return func(opt *formatOption) {
		opt.fmtOut = format
		opt.fmtExpect = format
	}
}

func AssertNilError(t *testing.T, out error) {
	Assert(t, out, nil, WithFormat(
		WithExpectOutputFormat("nil error"),
		WithOutputFormat("a non-nil error <%v>", out),
	))
}

func AssertBool(t *testing.T, out, expect bool) {
	Assert(t, out, expect, WithFormat(WithBothOutputFormat(FormatBool)))
}

func AssertInt(t *testing.T, out, expect int) {
	Assert(t, out, expect, WithFormat(WithBothOutputFormat(FormatDecimal)))
}

func AssertInt64(t *testing.T, out, expect int64) {
	Assert(t, out, expect, WithFormat(WithBothOutputFormat(FormatDecimal)))
}

func AssertNil(t *testing.T, out interface{}) {
	Assert(t, out, nil, WithFormat(WithExpectOutputFormat("<nil>")))
}

func Assert(t *testing.T, out, expect interface{}, options ...AssertOption) {
	o := NewAssertOption(options...)
	if !o.equalFunc(out, expect) {
		fmtOpts := make([]FormatOption, 0, 2+len(o.fmtOpts))
		fmtOpts = append(fmtOpts,
			WithOutputFormat(FormatDefault, out),
			WithExpectOutputFormat(FormatDefault, expect),
		)
		fmtOpts = append(fmtOpts, o.fmtOpts...)
		PrintErrorAndExit(t, fmtOpts...)
	}
}

func PrintErrorAndExit(t *testing.T, options ...FormatOption) {
	PrintError(t, options...)
	t.FailNow()
}

func PrintError(t *testing.T, options ...FormatOption) {
	fOpt := &formatOption{}
	for _, opt := range options {
		opt(fOpt)
	}

	var builder strings.Builder
	args := make([]interface{}, 0, 32)

	builder.WriteString("Failed: the result(s) should be ")
	builder.WriteString(string(fOpt.fmtExpect))
	args = append(args, fOpt.expect...)

	if len(fOpt.in) > 0 && len(fOpt.fmtIn) > 0 {
		builder.WriteString(" with input param(s) ")
		builder.WriteString(string(fOpt.fmtIn))
		args = append(args, fOpt.in...)
	}
	builder.WriteString(", got ")
	builder.WriteString(string(fOpt.fmtOut))
	args = append(args, fOpt.out...)

	builder.WriteString(".")

	t.Errorf(builder.String(), args...)
}
