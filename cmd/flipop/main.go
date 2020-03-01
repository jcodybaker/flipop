package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jcodybaker/flipop/pkg/controllers"
	"github.com/jcodybaker/flipop/pkg/log"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/sirupsen/logrus"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var debug bool
var ctx context.Context
var ll logrus.FieldLogger

var kubeconfig string

var rootCmd = &cobra.Command{
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if debug {
			logrus.SetLevel(logrus.DebugLevel)
		}
		if isatty.IsTerminal(os.Stdout.Fd()) {
			logrus.SetFormatter(&logrus.TextFormatter{})
		}
	},
	Run: runMain,
}

func init() {
	viper.SetEnvPrefix("flipop")

	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.InfoLevel)

	ctx = context.Background()
	ll = log.FromContext(ctx)
	ctx = log.AddToContext(signalContext(context.Background()), ll)
}

func main() {
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "debug logging")
	rootCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig file")
	rootCmd.Execute()
}

func signalContext(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1) // exit hard for the impatient
	}()

	return ctx
}

func runMain(cmd *cobra.Command, args []string) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	flipCtrl, err := controllers.NewFloatingIPPoolController(config, nil)
	if err != nil {
		fmt.Fprintf(os.Stdout, "Failed to create Floating IP Pool controller: %s", err)
	}
	flipCtrl.Run(ctx, ll)
}
