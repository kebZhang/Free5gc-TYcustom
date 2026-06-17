// Package dbtrace is a drop-in tracing wrapper around free5gc/util/mongoapi.
// Every exported function mirrors the signature of the corresponding mongoapi
// function, records the NF-side request/response timestamps and the targeted
// collection + UE id, emits one DB access-log entry (asynchronously, see
// accesslog), and then delegates to the real mongoapi call.
//
// Usage: replace `mongoapi.RestfulAPIXxx(` call sites with `dbtrace.RestfulAPIXxx(`.
// Signatures and semantics are identical, so this is a one-token change.
//
// Only the mongoapi functions actually used by the NFs are wrapped here.
package dbtrace

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/free5gc/udr/internal/accesslog"
	"github.com/free5gc/util/mongoapi"
)

// mongoTarget is the logged identifier for the database peer. MongoDB is a
// single logical store per deployment from the NF's point of view.
const mongoTarget = "mongodb"

// ueIDKeys are the bson filter keys, in priority order, that identify the UE a
// query concerns. free5gc filters use these across collections.
var ueIDKeys = []string{
	"ueId", "ueid", "supi", "Supi", "SUPI",
	"gpsi", "Gpsi", "pei", "servingPlmnId",
	"subscriptionId", "ipv4Addr", "imsi",
}

// ueIDFromFilter best-effort extracts the UE id from a query filter so the log
// can be keyed per UE. Returns "" when no recognizable id is present.
func ueIDFromFilter(filter bson.M) string {
	for _, k := range ueIDKeys {
		if v, ok := filter[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// logDB emits one DB record. start is captured by the caller right before the
// real call; we stamp the end here.
func logDB(collName string, filter bson.M, start time.Time) {
	accesslog.LogDB(mongoTarget, collName, ueIDFromFilter(filter), start, time.Now())
}

func RestfulAPIGetOne(collName string, filter bson.M, argOpt ...interface{}) (map[string]interface{}, error) {
	start := time.Now()
	res, err := mongoapi.RestfulAPIGetOne(collName, filter, argOpt...)
	logDB(collName, filter, start)
	return res, err
}

func RestfulAPIGetMany(collName string, filter bson.M, argOpt ...interface{}) ([]map[string]interface{}, error) {
	start := time.Now()
	res, err := mongoapi.RestfulAPIGetMany(collName, filter, argOpt...)
	logDB(collName, filter, start)
	return res, err
}

func RestfulAPIPutOne(
	collName string, filter bson.M, putData map[string]interface{}, argOpt ...interface{},
) (bool, error) {
	start := time.Now()
	existed, err := mongoapi.RestfulAPIPutOne(collName, filter, putData, argOpt...)
	logDB(collName, filter, start)
	return existed, err
}

func RestfulAPIDeleteOne(collName string, filter bson.M, argOpt ...interface{}) error {
	start := time.Now()
	err := mongoapi.RestfulAPIDeleteOne(collName, filter, argOpt...)
	logDB(collName, filter, start)
	return err
}

func RestfulAPIDeleteMany(collName string, filter bson.M, argOpt ...interface{}) error {
	start := time.Now()
	err := mongoapi.RestfulAPIDeleteMany(collName, filter, argOpt...)
	logDB(collName, filter, start)
	return err
}

func RestfulAPIMergePatch(
	collName string, filter bson.M, patchData map[string]interface{}, argOpt ...interface{},
) error {
	start := time.Now()
	err := mongoapi.RestfulAPIMergePatch(collName, filter, patchData, argOpt...)
	logDB(collName, filter, start)
	return err
}

func RestfulAPIJSONPatch(collName string, filter bson.M, patchJSON []byte, argOpt ...interface{}) error {
	start := time.Now()
	err := mongoapi.RestfulAPIJSONPatch(collName, filter, patchJSON, argOpt...)
	logDB(collName, filter, start)
	return err
}

func RestfulAPIJSONPatchExtend(
	collName string, filter bson.M, patchJSON []byte, dataName string, argOpt ...interface{},
) error {
	start := time.Now()
	err := mongoapi.RestfulAPIJSONPatchExtend(collName, filter, patchJSON, dataName, argOpt...)
	logDB(collName, filter, start)
	return err
}

func RestfulAPIPullOne(
	collName string, filter bson.M, putData map[string]interface{}, argOpt ...interface{},
) error {
	start := time.Now()
	err := mongoapi.RestfulAPIPullOne(collName, filter, putData, argOpt...)
	logDB(collName, filter, start)
	return err
}

func RestfulAPIPost(
	collName string, filter bson.M, postData map[string]interface{}, argOpt ...interface{},
) (bool, error) {
	start := time.Now()
	existed, err := mongoapi.RestfulAPIPost(collName, filter, postData, argOpt...)
	logDB(collName, filter, start)
	return existed, err
}
