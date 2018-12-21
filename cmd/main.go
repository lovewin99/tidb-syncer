package main

import (
	"flag"
	"fmt"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"tidb-syncer/river"
	"tidb-syncer/utils/logutil"
)

/**
Usage of syncer:
  -L string
      日志等级: debug, info, warn, error, fatal (默认为 "info")
  -V
      输出 syncer 版本；默认 false
  -auto-fix-gtid
      当 mysql master/slave 切换时，自动修复 gtid 信息；默认 false
  -b int
      batch 事务大小 (默认 10)
  -c int
      syncer 处理 batch 线程数 (默认 16)
  -config string
      指定相应配置文件启动 sycner 服务；如 `--config config.toml`
  -enable-gtid
      使用 gtid 模式启动 syncer；默认 false，开启前需要上游 MySQL 开启 GTID 功能
  -log-file string
      指定日志文件目录；如 `--log-file ./syncer.log`
  -log-rotate string
      指定日志切割周期, hour/day (默认 "day")
  -meta string
      指定 syncer 上游 meta 信息文件  (默认与配置文件相同目录下 "syncer.meta")
  -server-id int
     指定 MySQL slave sever-id (默认 101)
  -status-addr string
      指定 syncer metric 信息; 如 `--status-addr 127:0.0.1:10088`
*/
//Flag Names
const (
	logLevel    = "L"
	version     = "V"
	autoFixGtid = "auto-fix-gtid"
	batchSize   = "b"
	threadCount = "c"
	configPath  = "config"
	enableGtid  = "enable-gtid" //todo
	logPath     = "log-file"
	logRotate   = "log-rotate" //todo
	metaPath    = "meta"
	serverid    = "server-id"
	statusAddr  = "status-addr"
	isConsole   = "is_console"
)

var (
	prt_version  = flagBoolean(version, false, "print version information and exit")
	config_file  = flag.String(configPath, "config.toml", "tidb-syncer config file")
	batch_size   = flag.Int(batchSize, 0, "batch_size")
	server_id    = flag.Uint(serverid, 0, "MySQL server id, as a pseudo slave")
	log_level    = flag.String(logLevel, "", "log level")
	log_file     = flag.String(logPath, "", "log file path")
	log_rotate   = flag.String(logRotate, "", "log split")
	thread_count = flag.Int(threadCount, 0, "thread count")
	data_dir     = flag.String(metaPath, "", "meta file path")
	status_addr  = flag.String(statusAddr, "", "syncer metric info")
	is_console   = flag.Bool(isConsole, false, "log is or not in console")
)

func main() {

	flag.Parse()
	if *prt_version {
		fmt.Println("v1.1.1")
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*config_file)

	// use command line config
	overrideConfig(cfg)

	logutil.InitLogger(cfg, nil)

	runtime.GOMAXPROCS(cfg.WorkerCount)

	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	log.Debug(r)

	// start worker thread
	done := make(chan struct{}, 1)
	go func() {
		r.PreStartSync()
		r.Run()
		done <- struct{}{}
	}()

	// protection thread
	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	log.Debug("1 is stop!")

	r.Close()

	<-done

	log.Debug("all is stop!")
}

/**
Command line parameters override configuration file parameters
*/
func overrideConfig(cfg *river.Config) {
	actualFlags := make(map[string]bool)

	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	//log
	if actualFlags[logLevel] {
		cfg.LogLever = *log_level
	}

	if actualFlags[logPath] {
		cfg.LogPath = *log_file
	}

	if actualFlags[logRotate] && (*log_rotate == "day" || *log_rotate == "hour") {
		cfg.LogRotate = *log_rotate
	}

	if actualFlags[batchSize] {
		cfg.Batch = *batch_size
	}

	if actualFlags[serverid] {
		cfg.ServerID = uint32(*server_id)
	}

	if actualFlags[threadCount] {
		cfg.WorkerCount = *thread_count
	}

	if actualFlags[metaPath] {
		cfg.DataDir = *data_dir
	}

	if actualFlags[statusAddr] {
		cfg.StatAddr = *status_addr
	}

	if actualFlags[isConsole] {
		cfg.LogConsole = *is_console
	}

	return
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if defaultVal == false {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}
