package elogrus

import (
	"context"
	"fmt"
	"github.com/graniticio/granitic/ws/json"
	"os"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

var (
	// ErrCannotCreateIndex Fired if the index is not created
	ErrCannotCreateIndex = fmt.Errorf("cannot create index")
)

// IndexNameFunc get index name
type IndexNameFunc func() string

// MessageTransformer builds the payload that is sent to elasticsearch
type MessageTransformer func(entry *logrus.Entry, hook *ElasticHook) map[string]interface{}

// CreateHookOptions are the optional parameters for creating hooks
type CreateHookOptions struct {
	// Host defaults to the hostname of the current machine
	Host string
	// Level defaults to "panic"
	Level logrus.Level
	// Index defaults to "default"
	Index string
	// IndexNameFunc defaults to returning Index
	IndexNameFunc IndexNameFunc
	// MessageTransformer transforms the message payload sent to elasticsearch.
	// Defaults to the default message structure for this package
	MessageTransformer MessageTransformer
	// Bulk determines whether or not messages are sent out in bulk. Defaults to false
	Bulk bool
	// Async determines whether or not messages are sent out asynchronously. Defaults to false
	Async bool
}

var defaultCreateHookOptions CreateHookOptions = CreateHookOptions{
	// needs to use the os to get this
	Host:               "",
	Level:              logrus.InfoLevel,
	Index:              "default",
	MessageTransformer: defaultMessageTransformer,
}

type fireFunc func(entry *logrus.Entry, hook *ElasticHook) error

// ElasticHook is a logrus
// hook for ElasticSearch
type ElasticHook struct {
	client    *elastic.Client
	host      string
	index     IndexNameFunc
	levels    []logrus.Level
	ctx       context.Context
	ctxCancel context.CancelFunc
	fireFunc  fireFunc
	transformer MessageTransformer
}

// NewElasticHook creates new hook.
// client - ElasticSearch client with specific es version (v5/v6/v7/...)
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewElasticHook(client *elastic.Client, options CreateHookOptions) (*ElasticHook, error) {
	messageTransformer := options.MessageTransformer
	if messageTransformer == nil {
		messageTransformer = defaultCreateHookOptions.MessageTransformer
	}
	indexName := options.Index
	if indexName == "" {
		indexName = defaultCreateHookOptions.Index
	}
	fireFunction := syncFireFunc(messageTransformer)
	if options.Async {
		fireFunction = asyncFireFunc(messageTransformer)
	}
	indexNameFunction := defaultIndexNameFunc(indexName)
	if options.IndexNameFunc != nil {
		indexNameFunction = options.IndexNameFunc
	}
	host := options.Host
	if host == "" {
		h, err := os.Hostname()
		if err != nil {
			return nil, err
		}

		host = h
	}

	return newHookFuncAndFireFunc(client, host, options.Level, indexNameFunction, fireFunction)
}

// NewAsyncElasticHook creates new  hook with asynchronous log.
// client - ElasticSearch client with specific es version (v5/v6/v7/...)
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewAsyncElasticHook(client *elastic.Client, host string, level logrus.Level, index string) (*ElasticHook, error) {
	return NewAsyncElasticHookWithFunc(client, host, level, func() string { return index })
}

// NewBulkProcessorElasticHook creates new hook that uses a bulk processor for indexing.
// client - ElasticSearch client with specific es version (v5/v6/v7/...)
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewBulkProcessorElasticHook(client *elastic.Client, host string, level logrus.Level, index string) (*ElasticHook, error) {
	return NewBulkProcessorElasticHookWithFunc(client, host, level, func() string { return index })
}

// NewElasticHookWithFunc creates new hook with
// function that provides the index name. This is useful if the index name is
// somehow dynamic especially based on time.
// client - ElasticSearch client with specific es version (v5/v6/v7/...)
// host - host of system
// level - log level
// indexFunc - function providing the name of index
func NewElasticHookWithFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc) (*ElasticHook, error) {
	return newHookFuncAndFireFunc(client, host, level, indexFunc, defaultSyncFireFunc)
}

// NewAsyncElasticHookWithFunc creates new asynchronous hook with
// function that provides the index name. This is useful if the index name is
// somehow dynamic especially based on time.
// client - ElasticSearch client with specific es version (v5/v6/v7/...)
// host - host of system
// level - log level
// indexFunc - function providing the name of index
func NewAsyncElasticHookWithFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc) (*ElasticHook, error) {
	return newHookFuncAndFireFunc(client, host, level, indexFunc, defaultAsyncFireFunc)
}

// NewBulkProcessorElasticHookWithFunc creates new hook with
// function that provides the index name. This is useful if the index name is
// somehow dynamic especially based on time that uses a bulk processor for
// indexing.
// client - ElasticSearch client with specific es version (v5/v6/v7/...)
// host - host of system
// level - log level
// indexFunc - function providing the name of index
func NewBulkProcessorElasticHookWithFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc) (*ElasticHook, error) {
	fireFunc, err := makeBulkFireFunc(client, defaultMessageTransformer)
	if err != nil {
		return nil, err
	}
	return newHookFuncAndFireFunc(client, host, level, indexFunc, fireFunc)
}

func newHookFuncAndFireFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc, fireFunc fireFunc) (*ElasticHook, error) {
	var levels []logrus.Level
	for _, l := range []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
		logrus.TraceLevel,
	} {
		if l <= level {
			levels = append(levels, l)
		}
	}
	ctx, cancel := context.WithCancel(context.TODO())

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(indexFunc()).Do(ctx)
	if err != nil {
		// Handle error
		cancel()
		return nil, err
	}
	if !exists {
		createIndex, err := client.CreateIndex(indexFunc()).Do(ctx)
		if err != nil {
			cancel()
			return nil, err
		}
		if !createIndex.Acknowledged {
			cancel()
			return nil, ErrCannotCreateIndex
		}
	}

	return &ElasticHook{
		client:    client,
		host:      host,
		index:     indexFunc,
		levels:    levels,
		ctx:       ctx,
		ctxCancel: cancel,
		fireFunc:  fireFunc,
	}, nil
}

// Fire is required to implement
// Logrus hook
func (hook *ElasticHook) Fire(entry *logrus.Entry) error {
	return hook.fireFunc(entry, hook)
}

func asyncFireFunc(transformer MessageTransformer) fireFunc {
	return func (entry *logrus.Entry, hook *ElasticHook) error {
		go syncFireFunc(transformer)(entry, hook)
		return nil
	}
}

var defaultAsyncFireFunc = asyncFireFunc(defaultMessageTransformer)

func syncFireFunc(transformer MessageTransformer) fireFunc {
	return func (entry *logrus.Entry, hook *ElasticHook) error {
		_, err := hook.client.
			Index().
			Index(hook.index()).
			Type("log").
			BodyJson(transformer(entry, hook)).
			Do(hook.ctx)

		return err
	}
}

var defaultSyncFireFunc = syncFireFunc(defaultMessageTransformer)

// Create closure with bulk processor tied to fireFunc.
func makeBulkFireFunc(client *elastic.Client, transformer MessageTransformer) (fireFunc, error) {
	processor, err := client.BulkProcessor().
		Name("elogrus.v3.bulk.processor").
		Workers(2).
		FlushInterval(time.Second).
		Do(context.Background())

	return func(entry *logrus.Entry, hook *ElasticHook) error {
		r := elastic.NewBulkIndexRequest().
			Index(hook.index()).
			Type("log").
			Doc(transformer(entry, hook))
		processor.Add(r)
		return nil
	}, err
}

// Levels Required for logrus hook implementation
func (hook *ElasticHook) Levels() []logrus.Level {
	return hook.levels
}

// Cancel all calls to elastic
func (hook *ElasticHook) Cancel() {
	hook.ctxCancel()
}

// LowerCasedRootedMetadataMessageTransformer returns a message transformer that uses camelCased keys and keeps
// metadata on the root of the message object sent to elasticsearch
func LowerCasedRootedMetadataMessageTransformer(entry *logrus.Entry, hook *ElasticHook) map[string]interface{} {
	level := entry.Level.String()

	if e, ok := entry.Data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			entry.Data[logrus.ErrorKey] = err.Error()
		}
	}
	content, err := json.CamelCase(entry.Data)
	if err != nil {
		fmt.Printf("Struct -> Camel Case: %s", err)
	}

	result := map[string]interface{}{
		"host": hook.host,
		"@timestamp": entry.Time.UTC().Format(time.RFC3339Nano),
		"message": entry.Message,
		"level": strings.ToLower(level),
	}

	for key, value := range content.(map[string]interface{}) {
		if key != "host" && key != "@timestamp" && key != "message" && key != "level" {
			result[key] = value
		}
	}

	return result
}

func defaultIndexNameFunc(providedIndexName string) IndexNameFunc {
	return func() string {
		return providedIndexName
	}
}

func defaultMessageTransformer(entry *logrus.Entry, hook *ElasticHook) map[string]interface{} {
	level := entry.Level.String()

	if e, ok := entry.Data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			entry.Data[logrus.ErrorKey] = err.Error()
		}
	}

	return map[string]interface{}{
		"Host": hook.host,
		"@timestamp": entry.Time.UTC().Format(time.RFC3339Nano),
		"Message": entry.Message,
		"Level": strings.ToLower(level),
		"Data": entry.Data,
	}
}
