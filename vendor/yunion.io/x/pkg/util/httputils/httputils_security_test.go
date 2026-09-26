package httputils

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJSONClientErrorRedactsSignedURLAndCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer server.Close()

	url := server.URL + "/?Action=Describe&AccessKeyId=AKIA-SECRET&Signature=SIGNATURE-SECRET&X-Amz-Security-Token=SESSION-SECRET"
	req, err := Request(nil, t.Context(), GET, url, http.Header{
		"Authorization": []string{"Bearer header-secret"},
		"X-Api-Key":     []string{"api-secret"},
	}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ParseResponse("", req, nil, false)
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	got := err.Error()
	for _, secret := range []string{"AKIA-SECRET", "SIGNATURE-SECRET", "SESSION-SECRET", "header-secret", "api-secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("error leaked %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "Action=Describe") {
		t.Fatalf("safe query parameter was lost: %s", got)
	}
}

func TestJSONClientErrorRedactsSensitiveRequestBody(t *testing.T) {
	err := newJsonClientErrorFromRequest2(
		"POST",
		"https://cloud.example.test/",
		http.Header{"Content-Type": []string{"application/json"}},
		`{"AccessKeyId":"AKIA-BODY-SECRET","SecretAccessKey":"SECRET-BODY","Token":"TOKEN-BODY","safe":"ok"}`,
	)

	got := err.Error()
	for _, secret := range []string{"AKIA-BODY-SECRET", "SECRET-BODY", "TOKEN-BODY"} {
		if strings.Contains(got, secret) {
			t.Fatalf("error leaked %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, `"safe":"ok"`) {
		t.Fatalf("safe request field was lost: %s", got)
	}
}
