package aws

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws/request"
	"reflect"
	"testing"

	"yunion.io/x/pkg/errors"
)

func diskFilterFailure(code string) error {
	failure := &sAwsError{}
	failure.Errors.Code = code
	return failure
}

func volumePage(out interface{}, token string, volumes ...SDisk) error {
	response := struct {
		XMLName xml.Name `xml:"DescribeVolumesResponse"`
		Volumes []SDisk  `xml:"volumeSet>item"`
		Token   string   `xml:"nextToken"`
	}{Volumes: volumes, Token: token}
	data, err := xml.Marshal(response)
	if err != nil {
		return err
	}
	return xml.Unmarshal(data, out)
}

func TestDescribeVolumesZoneInternalErrorFallbackPreservesQuery(t *testing.T) {
	calls := 0
	disks, err := getDisksWithRequest("i-owner", "ap-northeast-1a", "gp2", []string{"vol-a", "vol-b"}, func(action string, params map[string]string, out interface{}) error {
		calls++
		if action != "DescribeVolumes" {
			t.Fatalf("unexpected action %s", action)
		}
		if calls == 1 {
			if params["Filter.2.Name"] != "availability-zone" || params["Filter.2.Value.1"] != "ap-northeast-1a" {
				t.Fatalf("initial zone filter missing: %v", params)
			}
			return errors.Wrap(diskFilterFailure("InternalError"), "provider")
		}
		want := map[string]string{"VolumeId.1": "vol-a", "VolumeId.2": "vol-b", "Filter.1.Name": "attachment.instance-id", "Filter.1.Value.1": "i-owner", "Filter.2.Name": "volume-type", "Filter.2.Value.1": "gp2"}
		if calls == 3 {
			want["NextToken"] = "page2"
		}
		if calls == 4 {
			want["NextToken"] = "page3"
		}
		if !reflect.DeepEqual(params, want) {
			t.Fatalf("fallback changed other filters or pagination: got=%v want=%v", params, want)
		}
		switch calls {
		case 2:
			return volumePage(out, "page2", SDisk{VolumeId: "vol-b", AvailabilityZone: "ap-northeast-1c"})
		case 3:
			return volumePage(out, "page3") // Empty page is not the end when a token exists.
		case 4:
			return volumePage(out, "", SDisk{VolumeId: "vol-a", AvailabilityZone: "ap-northeast-1a"})
		default:
			t.Fatalf("unexpected request %d", calls)
			return nil
		}
	})
	if err != nil || calls != 4 || len(disks) != 1 || disks[0].VolumeId != "vol-a" {
		t.Fatalf("calls=%d disks=%#v error=%v", calls, disks, err)
	}
}

func TestDescribeVolumesDoesNotFallbackOtherErrors(t *testing.T) {
	for _, failure := range []error{diskFilterFailure("AccessDenied"), diskFilterFailure("UnauthorizedOperation"), diskFilterFailure("AuthFailure"), fmt.Errorf("InternalError"), fmt.Errorf("connection reset")} {
		calls := 0
		disks, err := getDisksWithRequest("", "ap-northeast-1a", "", nil, func(string, map[string]string, interface{}) error { calls++; return failure })
		if calls != 1 || disks != nil || errors.Cause(err) != failure {
			t.Fatalf("failure=%v calls=%d disks=%v err=%v", failure, calls, disks, err)
		}
	}
	calls := 0
	_, err := getDisksWithRequest("", "", "", nil, func(string, map[string]string, interface{}) error { calls++; return diskFilterFailure("InternalError") })
	if calls != 1 || err == nil {
		t.Fatalf("no-zone request must not fallback: calls=%d err=%v", calls, err)
	}
}

func TestDescribeVolumesFallbackNeverReturnsPartialInventory(t *testing.T) {
	for _, missingZone := range []bool{false, true} {
		calls := 0
		disks, err := getDisksWithRequest("", "ap-northeast-1a", "", nil, func(_ string, _ map[string]string, out interface{}) error {
			calls++
			switch calls {
			case 1:
				return diskFilterFailure("InternalError")
			case 2:
				return volumePage(out, "next", SDisk{VolumeId: "vol-a", AvailabilityZone: "ap-northeast-1a"})
			case 3:
				if missingZone {
					return volumePage(out, "", SDisk{VolumeId: "vol-unknown"})
				}
				return diskFilterFailure("AccessDenied")
			default:
				t.Fatal("unexpected retry")
				return nil
			}
		})
		if calls != 3 || disks != nil || err == nil {
			t.Fatalf("missingZone=%v calls=%d disks=%v err=%v", missingZone, calls, disks, err)
		}
	}
}

func TestDescribeVolumesLateFallbackRestartsPagination(t *testing.T) {
	calls := 0
	disks, err := getDisksWithRequest("", "ap-northeast-1a", "", nil, func(_ string, params map[string]string, out interface{}) error {
		calls++
		switch calls {
		case 1:
			return volumePage(out, "old-page2", SDisk{VolumeId: "stale", AvailabilityZone: "ap-northeast-1a"})
		case 2:
			return diskFilterFailure("InternalError")
		case 3:
			if len(params) != 0 {
				t.Fatalf("fallback retained stale token/zone: %v", params)
			}
			return volumePage(out, "", SDisk{VolumeId: "current", AvailabilityZone: "ap-northeast-1a"})
		default:
			t.Fatal("unexpected request")
			return nil
		}
	})
	if err != nil || len(disks) != 1 || disks[0].VolumeId != "current" {
		t.Fatalf("partial prior query leaked: %#v %v", disks, err)
	}
}

func TestDescribeVolumesFallbackAllowsAuthoritativeEmptyZone(t *testing.T) {
	calls := 0
	disks, err := getDisksWithRequest("", "ap-northeast-1a", "", nil, func(_ string, _ map[string]string, out interface{}) error {
		calls++
		if calls == 1 {
			return diskFilterFailure("InternalError")
		}
		return volumePage(out, "", SDisk{VolumeId: "vol-c", AvailabilityZone: "ap-northeast-1c"}, SDisk{VolumeId: "vol-d", AvailabilityZone: "ap-northeast-1d"})
	})
	if err != nil || calls != 2 || len(disks) != 0 {
		t.Fatalf("complete inventory proves zone empty: calls=%d disks=%v err=%v", calls, disks, err)
	}
}

func TestDescribeVolumesFallbackDoesNotRetryInternalErrorWithoutZone(t *testing.T) {
	calls := 0
	disks, err := getDisksWithRequest("", "ap-northeast-1a", "", nil, func(string, map[string]string, interface{}) error { calls++; return diskFilterFailure("InternalError") })
	if calls != 2 || disks != nil || err == nil {
		t.Fatalf("fallback must fail once: calls=%d disks=%v err=%v", calls, disks, err)
	}
}

func TestDescribeVolumesFallbackUsesUnmarshaledEC2Error(t *testing.T) {
	calls := 0
	disks, err := getDisksWithRequest("", "ap-northeast-1a", "", nil, func(_ string, _ map[string]string, out interface{}) error {
		calls++
		if calls == 1 {
			req, _ := http.NewRequest(http.MethodPost, "https://ec2.ap-northeast-1.amazonaws.com/", nil)
			response := &request.Request{HTTPRequest: req, HTTPResponse: &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(`<Response><Errors><Error><Code>InternalError</Code><Message>Internal error</Message></Error></Errors><RequestID>test-id</RequestID></Response>`))}}
			UnmarshalError(response)
			if response.Error == nil {
				t.Fatal("EC2 error XML was not decoded")
			}
			return response.Error
		}
		return volumePage(out, "", SDisk{VolumeId: "vol-a", AvailabilityZone: "ap-northeast-1a"})
	})
	if err != nil || calls != 2 || len(disks) != 1 {
		t.Fatalf("actual error decoder did not trigger fallback: calls=%d disks=%v err=%v", calls, disks, err)
	}
}
