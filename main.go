package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"code.cloudfoundry.org/lager"
	forceexport "github.com/alangpierce/go-forceexport"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/event"
	"github.com/concourse/concourse/atc/metric"
	_ "github.com/concourse/concourse/atc/metric/emitter"
	"github.com/concourse/flag"
	flags "github.com/jessevdk/go-flags"
	"github.com/vito/twentythousandtonnesofcrudeoil"
)

type MetricDrainCommand struct {
	Logger   flag.Lager
	Postgres flag.PostgresConfig `group:"PostgreSQL Configuration" namespace:"postgres"`
	Metrics  struct {
		HostName   string            `long:"metrics-host-name" description:"Host string to attach to emitted metrics."`
		Attributes map[string]string `long:"metrics-attribute" description:"A key-value attribute to attach to emitted metrics. Can be specified multiple times." value-name:"NAME:VALUE"`
	} `group:"Metrics & Diagnostics"`
}

func (cmd *MetricDrainCommand) Execute() error {
	logger, _ := cmd.Logger.Logger("metricdrain")
	host := cmd.Metrics.HostName
	if host == "" {
		host, _ = os.Hostname()
	}
	err := metric.Initialize(
		logger,
		host,
		cmd.Metrics.Attributes,
	)
	if err != nil {
		return err
	}
	driverName := "too-many-connections-retrying"
	db.SetupConnectionRetryingDriver(
		"postgres",
		cmd.Postgres.ConnectionString(),
		driverName,
	)
	lockConn, err := sql.Open(driverName, cmd.Postgres.ConnectionString())
	if err != nil {
		return err
	}
	lockConn.SetMaxOpenConns(1)
	lockConn.SetMaxIdleConns(1)
	lockConn.SetConnMaxLifetime(0)

	lockFactory := lock.NewLockFactory(lockConn, metric.LogLockAcquired, metric.LogLockReleased)
	dbConn, err := db.Open(logger.Session("db"), driverName, cmd.Postgres.ConnectionString(), nil, nil, "builds", lockFactory)
	if err != nil {
		return err
	}

	buildFactory := db.NewBuildFactory(dbConn, lockFactory, 5*time.Minute)
	builds, err := buildFactory.GetDrainableBuilds()
	if err != nil {
		logger.Error("failed-to-get-drainable-builds", err)
		return err
	}

	for _, build := range builds {
		err := cmd.drainBuild(logger, build)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cmd *MetricDrainCommand) drainBuild(logger lager.Logger, build db.Build) error {
	logger = logger.Session("drain-build", lager.Data{
		"team":     build.TeamName(),
		"pipeline": build.PipelineName(),
		"job":      build.JobName(),
		"build":    build.Name(),
	})

	events, err := build.Events(0)
	if err != nil {
		return err
	}

	// ignore any errors coming from events.Close()
	defer db.Close(events)

	for {
		ev, err := events.Next()
		if err != nil {
			if err == db.ErrEndOfBuildEventStream {
				break
			}
			logger.Error("failed-to-get-next-event", err)
			return err
		}

		// for a demo, let's not clog things up with logs
		if ev.Event == event.EventTypeLog {
			continue
		}

		// TODO: this is heinous treachery, but works for a demo
		var metricEmit func(lager.Logger, metric.Event)
		err = forceexport.GetFunc(&metricEmit, "github.com/concourse/concourse/atc/metric.emit")
		if err != nil {
			return err
		}

		var publicPlan atc.Plan
		json.Unmarshal(*build.PublicPlan(), &publicPlan) // TODO: do builds ever lose their public plan?

		metricEmit(logger.Session("build-event"), metric.Event{
			Name: "build event",
			Attributes: map[string]string{
				"team":     build.TeamName(),
				"pipeline": build.PipelineName(),
				"job":      build.JobName(),
				"build":    build.Name(),
				"step":     publicPlan.Task.Name, // TODO: this only works for tasks!!!
				"type":     string(ev.Event),
				// "data":     string(*ev.Data),
			},
		})
	}

	// TODO: uncomment this part in production LOL
	// err = build.SetDrained(true)
	// if err != nil {
	// 	logger.Error("failed-to-update-status", err)
	// 	return err
	// }

	return nil
}

func main() {
	var cmd MetricDrainCommand
	parser := flags.NewParser(&cmd, flags.HelpFlag|flags.PassDoubleDash)
	parser.NamespaceDelimiter = "-"
	for _, group := range parser.Command.Groups()[0].Groups() {
		if group.ShortDescription == "Metrics & Diagnostics" {
			metric.WireEmitters(group)
		}
	}
	twentythousandtonnesofcrudeoil.TheEnvironmentIsPerfectlySafe(parser, "CONCOURSE_")
	_, err := parser.Parse()
	handleError(parser, err)
	err = cmd.Execute()
	handleError(parser, err)
	// awkwardly waiting for a private channel to be empty
	time.Sleep(500 * time.Millisecond)
}

func handleError(helpParser *flags.Parser, err error) {
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			fmt.Println(err)
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
		}

		os.Exit(1)
	}
}
