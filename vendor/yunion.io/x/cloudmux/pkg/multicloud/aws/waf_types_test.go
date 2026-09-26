package aws

import (
	"bytes"
	"testing"

	"yunion.io/x/jsonutils"
)

func TestDecodeWebACLByteMatchResponse(t *testing.T) {
	response, err := jsonutils.ParseString(`{"WebACL":{"Id":"acl-id","Name":"acl","DefaultAction":{"Allow":{}},"CustomResponseBodies":{"block_response":{"Content":"The request is blocked, Please try again later.","ContentType":"TEXT_PLAIN"}},"Rules":[{"Name":"match","Priority":1,"Action":{"Block":{}},"Statement":{"ByteMatchStatement":{"SearchString":"L2xvZ2lu","PositionalConstraint":"STARTS_WITH","FieldToMatch":{"UriPath":{}},"TextTransformations":[{"Priority":0,"Type":"NONE"}]}}}]},"LockToken":"token"}`)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		WebACL    *SWafWebACL
		LockToken string
	}
	if err := response.Unmarshal(&result); err != nil {
		t.Fatalf("decode GetWebACL response: %v", err)
	}
	if result.WebACL == nil || len(result.WebACL.Rules) != 1 {
		t.Fatalf("missing WebACL rule: %+v", result)
	}
	match := result.WebACL.Rules[0].Statement.ByteMatchStatement
	if match == nil || string(match.SearchString) != "/login" {
		t.Fatalf("search bytes not decoded: %+v", match)
	}
}

func TestWafSearchStringRoundTrip(t *testing.T) {
	for _, value := range [][]byte{[]byte("/login"), {0, 255, 128, 1}, {}} {
		statement := sWafByteMatchStatement{SearchString: value}
		encoded := jsonutils.Marshal(statement)
		// The AWS blob representation must be a string, not an array of integers.
		if _, err := encoded.GetString("SearchString"); err != nil {
			t.Fatalf("AWS search string was not base64 JSON: %s: %v", encoded, err)
		}
		var decoded sWafByteMatchStatement
		if err := encoded.Unmarshal(&decoded); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decoded.SearchString, value) {
			t.Fatalf("search bytes changed: got %v, want %v", decoded.SearchString, value)
		}
	}
}

func TestWafSearchStringRejectsInvalidBase64(t *testing.T) {
	response, err := jsonutils.ParseString(`{"ByteMatchStatement":{"SearchString":"!invalid-base64!"}}`)
	if err != nil {
		t.Fatal(err)
	}
	var statement sWafStatement
	if err := response.Unmarshal(&statement); err == nil {
		t.Fatal("invalid AWS blob was silently accepted")
	}
}
