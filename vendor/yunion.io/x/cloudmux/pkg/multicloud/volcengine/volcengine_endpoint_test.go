package volcengine

import "testing"

func TestGetAPIEndpoint(t *testing.T) {
	client := &SVolcEngineClient{}
	tests := []struct {
		region  string
		service string
		want    string
	}{
		{"", VOLCENGINE_SERVICE_ECS, VOLCENGINE_API},
		{"ap-southeast-3", VOLCENGINE_SERVICE_ECS, "ecs.ap-southeast-3.volcengineapi.com"},
		{"cn-guangzhou", VOLCENGINE_SERVICE_VPC, "vpc.cn-guangzhou.volcengineapi.com"},
		{"ap-southeast-3", VOLCENGINE_SERVICE_NAT, "natgateway.ap-southeast-3.volcengineapi.com"},
		{"ap-southeast-3", VOLCENGINE_SERVICE_STORAGE, "storage-ebs.ap-southeast-3.volcengineapi.com"},
		{"ap-southeast-3", VOLCENGINE_SERVICE_MONITOR, "cloudmonitor.ap-southeast-3.volcengineapi.com"},
		{"ap-southeast-3", VOLCENGINE_SERVICE_IAM, VOLCENGINE_API},
	}
	for _, tt := range tests {
		if got := client.getAPIEndpoint(tt.region, tt.service); got != tt.want {
			t.Errorf("getAPIEndpoint(%q, %q) = %q, want %q", tt.region, tt.service, got, tt.want)
		}
	}
}
