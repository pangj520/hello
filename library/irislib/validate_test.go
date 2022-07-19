package irislib

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"testing"
)

func TestRequiredWithParent(t *testing.T) {
	//test.InitDomain()
	InitValidator()
	vs := struct {
		Port string `json:"port"`
	}{
		Port: "8080",
	}
	err := Validator.Struct(&vs)
	if err != nil {
		fmt.Println(ParseErrorMsg(err))
	}
}

func TestK8sLabel(t *testing.T) {
	//test.InitDomain()
	InitValidator()
	labels := []string{
		"a", "中文", "a:b", "A:Ba", "aA:Bb",
		"0a:1b", "中a:ab", "ab/bc:cd", "a-b:b-c", "a_b:B_c",
		"-a:b", "_a:b", "a:-b", "a:",
	}
	for i, label := range labels {
		vs := struct {
			Label string `json:"label" validate:"required,k8s_label"`
		}{
			Label: label,
		}
		err := Validator.Struct(&vs)
		if err != nil {
			fmt.Print("index ", i, ": ")
			fmt.Println(ParseErrorMsg(err))
		}
	}
}

func TestVarValue(t *testing.T) {
	//test.InitDomain()
	InitValidator()
	vars := []string{
		"afd'sf", "中fasdfdsaf文", "a/", `dfasf"dsfd`, "dfsadfa",
	}
	for i, label := range vars {
		vs := struct {
			VarV string `json:"varv" validate:"required,varValue"`
		}{
			VarV: label,
		}
		err := Validator.Struct(&vs)
		if err != nil {
			fmt.Print("index ", i, ": ")
			fmt.Println(ParseErrorMsg(err))
		}
	}
}

func TestUUID(t *testing.T) {
	fmt.Println(uuid.NewV4().String())
}
