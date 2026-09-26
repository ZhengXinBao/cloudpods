package main

import (
	"os"
	_ "yunion.io/x/cloudmux/pkg/multicloud/loader"
	"yunion.io/x/log"
	"yunion.io/x/onecloud/pkg/compute/service"
	_ "yunion.io/x/onecloud/pkg/mcclient/cloudpods/provider"
	"yunion.io/x/onecloud/pkg/util/atexit"
)

func main() {
	defer atexit.Handle()
	if err := service.StartSyncWorker(); err != nil {
		log.Errorf("sync worker: %v", err)
		os.Exit(1)
	}
}
