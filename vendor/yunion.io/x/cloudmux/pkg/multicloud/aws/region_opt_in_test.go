package aws

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"yunion.io/x/cloudmux/pkg/cloudprovider"
)

func TestDescribeRegionsUsesTargetAccountCredentials(t *testing.T) {
	for _, account := range []string{"", "123456789012"} {
		t.Run(account, func(t *testing.T) {
			var mu sync.Mutex
			var hosts []string
			cfg := NewAwsClientConfig(AWS_INTERNATIONAL_CLOUDENV, "test-key", "test-secret", account)
			cfg.CloudproviderConfig(cloudprovider.ProviderConfig{ProxyFunc: func(req *http.Request) (*url.URL, error) {
				mu.Lock()
				hosts = append(hosts, req.URL.Host)
				mu.Unlock()
				return nil, errors.New("test blocks outbound traffic")
			}})
			client := SAwsClient{AwsClientConfig: cfg}
			if _, err := client.GetRegions(); err == nil {
				t.Fatal("credential/request failure must propagate")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(hosts) == 0 {
				t.Fatal("no credential or API request attempted")
			}
			want := "ec2."
			if account != "" {
				want = "sts."
			}
			for _, host := range hosts {
				if !strings.HasPrefix(host, want) {
					t.Errorf("account=%q contacted %q; expected %s credentials", account, host, want)
				}
			}
		})
	}
}

func TestRegionOptInMetadata(t *testing.T) {
	for _, status := range []string{"not-opted-in", "opted-in", "opt-in-not-required", ""} {
		region := SRegion{OptInStatus: status}
		if region.GetRegionOptInStatus() != status {
			t.Fatal("opt-in metadata changed")
		}
	}
}
