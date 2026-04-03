package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	srcEndpoints := flag.String("src-endpoints", "", "source etcd endpoints (comma-separated, required)")
	dstEndpoints := flag.String("dst-endpoints", "", "destination etcd endpoints (comma-separated, required)")
	srcRootPath := flag.String("src-root-path", "by-dev", "source Milvus etcd rootPath")
	dstRootPath := flag.String("dst-root-path", "by-dev", "destination Milvus etcd rootPath")
	collection := flag.String("collection", "", "collection name to verify (required)")
	dbName := flag.String("db", "", "database name (optional, scans all databases if empty)")
	timeout := flag.Duration("timeout", 30*time.Second, "etcd operation timeout")
	flag.Parse()

	if *collection == "" || *srcEndpoints == "" || *dstEndpoints == "" {
		fmt.Fprintf(os.Stderr, "Usage: etcd-schema-verify --src-endpoints=HOST:PORT --dst-endpoints=HOST:PORT --collection=NAME\n\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	srcCli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*srcEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fatal("connect source etcd: %v", err)
	}
	defer srcCli.Close()

	dstCli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*dstEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fatal("connect destination etcd: %v", err)
	}
	defer dstCli.Close()

	srcReader := &reader{cli: srcCli, rootPath: *srcRootPath}
	srcDump, err := srcReader.findCollection(ctx, *dbName, *collection)
	if err != nil {
		fatal("source: %v", err)
	}

	dstReader := &reader{cli: dstCli, rootPath: *dstRootPath}
	dstDump, err := dstReader.findCollection(ctx, *dbName, *collection)
	if err != nil {
		fatal("target: %v", err)
	}

	diffs := diffCollections(srcDump, dstDump)

	result := &VerifyResult{
		Collection: *collection,
		Database:   *dbName,
		Aligned:    len(diffs) == 0,
		Source:     srcDump,
		Target:     dstDump,
		Diffs:      diffs,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fatal("encode JSON: %v", err)
	}

	if !result.Aligned {
		os.Exit(1)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
