package register

import (
	"github.com/spf13/cobra"
	"github.com/tidb-incubator/Serverlessdb-for-HTAP/pkg/he3local/scheduler"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func Register() *cobra.Command {
	return app.NewSchedulerCommand(
		app.WithPlugin(scheduler.Name, scheduler.New),
	)
}
