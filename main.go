package main

import (
	"context"
	"flag"
	"time"

	"github.com/google/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Domain = "cadence-system"
var HostPort = "127.0.0.1:7933"
var TaskListName = "SimpleWorker"
var ClientName = "cadence-client"
var CadenceService = "cadence-frontend"

func startWorkflow() {
	workflowOptions := client.StartWorkflowOptions{
		ID:                              "helloworld_" + uuid.New().String(),
		TaskList:                        TaskListName,
		ExecutionStartToCloseTimeout:    time.Minute,
		DecisionTaskStartToCloseTimeout: time.Minute,
	}

	logger := buildLogger()
	c := client.NewClient(buildCadenceClient(), Domain, &client.Options{})
	we, err := c.StartWorkflow(context.Background(), workflowOptions, helloWorldWorkflow, "hola")
	if err != nil {
		logger.Error("failed to create workflow", zap.Error(err))
	} else {
		logger.Info("started workflow", zap.String("workflowID", we.ID), zap.String("RunID", we.RunID))
	}
}

func main() {
	var mode string
	flag.StringVar(&mode, "m", "trigger", "Mode is worker or trigger.")
	flag.Parse()

	switch mode {
	case "worker":
		startWorker(buildLogger(), buildCadenceClient(), func(w worker.Worker) {
			w.RegisterActivity(helloWorldActivity)
			w.RegisterWorkflow(helloWorldWorkflow)
		})

		// The workers are supposed to be long running process that should not exit.
		// Use select{} to block indefinitely for samples, you can quit by CMD+C.
		select {}
	case "trigger":
		startWorkflow()
	}
}

func buildLogger() *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)

	var err error
	logger, err := config.Build()
	if err != nil {
		panic("Failed to setup logger")
	}

	return logger
}

func buildCadenceClient() workflowserviceclient.Interface {
	ch, err := tchannel.NewChannelTransport(tchannel.ServiceName(ClientName))
	if err != nil {
		panic("Failed to setup tchannel")
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: ClientName,
		Outbounds: yarpc.Outbounds{
			CadenceService: {Unary: ch.NewSingleOutbound(HostPort)},
		},
	})
	if err := dispatcher.Start(); err != nil {
		panic("Failed to start dispatcher")
	}

	return workflowserviceclient.New(dispatcher.ClientConfig(CadenceService))
}

func startWorker(logger *zap.Logger, service workflowserviceclient.Interface, fn func(worker.Worker)) {
	// TaskListName identifies set of client workflows, activities, and workers.
	// It could be your group or client or application name.
	workerOptions := worker.Options{
		Logger:       logger,
		MetricsScope: tally.NewTestScope(TaskListName, map[string]string{}),
	}

	worker := worker.New(
		service,
		Domain,
		TaskListName,
		workerOptions)
	fn(worker)
	err := worker.Start()
	if err != nil {
		panic("Failed to start worker")
	}
	logger.Info("Started Worker.", zap.String("worker", TaskListName))
}
