/*
 Copyright 2022 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/GoogleCloudPlatform/knfsd-cache-utils/image/resources/knfsd-fsidd/internal/metrics"
	"github.com/GoogleCloudPlatform/knfsd-cache-utils/image/resources/knfsd-fsidd/log"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

//go:embed schema.sql
var tableSchema string

func connect(ctx context.Context, config DatabaseConfig) (*pgxpool.Pool, error) {
	pgConfig, err := pgxpool.ParseConfig(config.URL)
	if err != nil {
		return nil, err
	}

	log.Debug.Print("Creating pgxpool")
	db, err := pgxpool.ConnectConfig(ctx, pgConfig)
	if err != nil {
		return nil, err
	}

	return db, err
}

type FSIDSource struct {
	db        *pgxpool.Pool
	tableName string
}

func (s FSIDSource) CreateTable(ctx context.Context) error {
	log.Debug.Printf("creating table \"%s\"", s.tableName)

	t, err := template.New("schema").Parse(tableSchema)
	if err != nil {
		return err
	}

	w := &strings.Builder{}
	err = t.Execute(w, s.tableName)
	if err != nil {
		return err
	}

	sql := w.String()
	return withRetry(ctx, func() error {
		_, err = s.db.Exec(ctx, sql)
		return err
	})
}

func (s FSIDSource) GetFSID(ctx context.Context, path string) (int32, error) {
	var fsid int32
	start := time.Now()
	sql := fmt.Sprintf("SELECT fsid FROM \"%s\" WHERE path = $1", s.tableName)
	row := s.db.QueryRow(ctx, sql, path)
	err := row.Scan(&fsid)
	metrics.SQLOperation(ctx, "get_fsid", SQLMetricResult(err), time.Since(start))
	return fsid, err
}

func (s FSIDSource) AllocateFSID(ctx context.Context, path string) (int32, error) {
	var fsid int32
	start := time.Now()
	sql := fmt.Sprintf("INSERT INTO \"%s\" (path) VALUES ($1) RETURNING fsid", s.tableName)
	row := s.db.QueryRow(ctx, sql, path)
	err := row.Scan(&fsid)
	metrics.SQLOperation(ctx, "allocate_fsid", SQLMetricResult(err), time.Since(start))
	return fsid, err
}

func (s FSIDSource) GetPath(ctx context.Context, fsid int32) (string, error) {
	var path string
	start := time.Now()
	sql := fmt.Sprintf("SELECT path FROM \"%s\" WHERE fsid = $1", s.tableName)
	row := s.db.QueryRow(ctx, sql, fsid)
	err := row.Scan(&path)
	metrics.SQLOperation(ctx, "get_path", SQLMetricResult(err), time.Since(start))
	return path, err
}

func IsConflict(err error) bool {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		// unique constraint violation
		return pgerr.Code == "23505"
	} else {
		return false
	}
}

func IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func SQLMetricResult(err error) string {
	if err == nil {
		return "ok"
	} else if IsNotFound(err) {
		return "not_found"
	} else if IsConflict(err) {
		return "conflict"
	} else {
		return "error"
	}
}
