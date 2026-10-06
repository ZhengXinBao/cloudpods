package ksyun

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/pkg/errors"
)

type diskLookupTransport func(*http.Request) (*http.Response, error)

func (f diskLookupTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRegionGetIDiskById(t *testing.T) {
	// UUID-shaped EBS IDs must be queried, not mistaken for emulated local disks.
	const diskID = "12345678-1234-1234-1234-123456789abc"
	const volume = `{"VolumeId":"` + diskID + `","VolumeName":"test-disk","Size":80,"VolumeStatus":"inuse","VolumeType":"SSD3.0","AvailabilityZone":"eu-east-1a","VolumeCategory":"data"}`
	const success = `{"Volumes":[` + volume + `],"TotalCount":1}`
	for _, tt := range []struct {
		name         string
		id           string
		responses    []string
		status       int
		transportErr error
		wantErr      string
		wantNotFound bool
	}{
		{name: "found with storage association", id: diskID, responses: []string{success}},
		{
			name: "exact ID on second page", id: diskID,
			responses: []string{
				`{"Volumes":[{"VolumeId":"another-disk"}],"TotalCount":2,"Marker":1}`,
				`{"Volumes":[` + volume + `],"TotalCount":2,"Marker":2}`,
			},
		},
		{name: "empty result", id: diskID, responses: []string{`{"Volumes":[],"TotalCount":0}`}, wantNotFound: true},
		{name: "different ID", id: diskID, responses: []string{`{"Volumes":[{"VolumeId":"another-disk"}],"TotalCount":1}`}, wantNotFound: true},
		{
			name: "permission denied", id: diskID, status: http.StatusForbidden,
			responses: []string{`{"Error":{"Code":"AccessDenied","Message":"Access denied"}}`},
			wantErr:   "AccessDenied",
		},
		{
			name: "service failure", id: diskID, status: http.StatusInternalServerError,
			responses: []string{`{"Error":{"Code":"InternalError","Message":"service unavailable"}}`},
			wantErr:   "InternalError",
		},
		{name: "network failure", id: diskID, transportErr: fmt.Errorf("network unavailable"), wantErr: "network unavailable"},
		{name: "malformed JSON", id: diskID, responses: []string{`not-json`}, wantErr: "json"},
		{name: "truncated JSON", id: diskID, responses: []string{`{"Volumes":`}, wantErr: "json"},
		{name: "empty response", id: diskID, responses: []string{``}, wantErr: "empty"},
		{
			name: "missing zone is not missing disk", id: diskID,
			responses: []string{strings.Replace(success, `"AvailabilityZone":"eu-east-1a"`, `"AvailabilityZone":""`, 1)},
			wantErr:   "availability zone",
		},
		{
			name: "missing type is not missing disk", id: diskID,
			responses: []string{strings.Replace(success, `"VolumeType":"SSD3.0"`, `"VolumeType":""`, 1)},
			wantErr:   "volume type",
		},
		{name: "empty ID makes no request", wantErr: "empty disk id"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			region := &SRegion{
				Region: "eu-east-1",
				client: &SKsyunClient{
					KsyunClientConfig: &KsyunClientConfig{
						accessKeyId:     "test-key",
						accessKeySecret: "test-secret",
						cpcfg:           cloudprovider.ProviderConfig{Id: "test-account"},
					},
					ctx: context.Background(),
					client: &http.Client{Transport: diskLookupTransport(func(req *http.Request) (*http.Response, error) {
						requests++
						q := req.URL.Query()
						if req.Method != http.MethodGet || q.Get("Action") != "DescribeVolumes" ||
							q.Get("VolumeId.1") != tt.id || q.Get("Region") != "eu-east-1" ||
							q.Get("Service") != "ebs" || q.Get("MaxResults") != "1000" {
							t.Fatalf("unexpected disk lookup request: method=%s action=%s id=%s region=%s",
								req.Method, q.Get("Action"), q.Get("VolumeId.1"), q.Get("Region"))
						}
						if q.Get("VolumeType") != "" {
							t.Fatal("region lookup must not restrict the previous storage type")
						}
						if requests == 2 && q.Get("Marker") != "1" {
							t.Fatalf("second page marker = %q, want 1", q.Get("Marker"))
						}
						if tt.transportErr != nil {
							return nil, tt.transportErr
						}
						if requests > len(tt.responses) {
							t.Fatalf("unexpected extra request %d", requests)
						}
						status := tt.status
						if status == 0 {
							status = http.StatusOK
						}
						return &http.Response{
							StatusCode: status,
							Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
							Header:     http.Header{"Content-Type": []string{"application/json"}},
							Body:       io.NopCloser(strings.NewReader(tt.responses[requests-1])),
							Request:    req,
						}, nil
					})},
				},
			}
			// Exercise the same region interface that cloudpods' syncRemoveCloudDisk uses.
			var iregion cloudprovider.ICloudRegion = region
			disk, err := iregion.GetIDiskById(tt.id)
			if tt.wantNotFound {
				if errors.Cause(err) != cloudprovider.ErrNotFound || disk != nil {
					t.Fatalf("got disk=%v err=%v, want nil disk and ErrNotFound", disk, err)
				}
			} else if tt.wantErr != "" {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
					t.Fatalf("got error %v, want %q", err, tt.wantErr)
				}
				if errors.Cause(err) == cloudprovider.ErrNotFound || disk != nil {
					t.Fatalf("query/metadata failure must not imply disk deletion: disk=%v err=%v", disk, err)
				}
			} else {
				if err != nil {
					t.Fatalf("GetIDiskById: %v", err)
				}
				if disk.GetGlobalId() != diskID || disk.GetName() != "test-disk" || disk.GetDiskSizeMB() != 80*1024 {
					t.Fatalf("unexpected disk metadata: id=%s name=%s size=%d", disk.GetGlobalId(), disk.GetName(), disk.GetDiskSizeMB())
				}
				storage, err := disk.GetIStorage()
				if err != nil {
					t.Fatalf("GetIStorage: %v", err)
				}
				wantStorage := &SStorage{
					zone:        &SZone{region: region, AvailabilityZone: "eu-east-1a"},
					StorageType: "SSD3.0",
				}
				if disk.GetIStorageId() != wantStorage.GetGlobalId() || storage.GetGlobalId() != wantStorage.GetGlobalId() {
					t.Fatalf("storage ID = %s, want %s", disk.GetIStorageId(), wantStorage.GetGlobalId())
				}
				if storage.GetStorageType() != "SSD3.0" || storage.GetIZone().GetId() != "eu-east-1a" ||
					storage.GetIZone().GetIRegion() != region {
					t.Fatal("disk is not associated with the correct type, zone and region")
				}
			}
			wantRequests := len(tt.responses)
			if tt.transportErr != nil {
				wantRequests = 1
			}
			if requests != wantRequests {
				t.Fatalf("made %d requests, want %d", requests, wantRequests)
			}
		})
	}
}
