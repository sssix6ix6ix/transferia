package greenplum

import (
	"context"
	"sort"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"go.ytsaurus.tech/library/go/core/log"
)

// transferObjectsPreUpload applies DDL for object types that must exist
// before data upload (schemas, types, functions, sequences, etc.).
func transferObjectsPreUpload(ctx context.Context, lgr log.Logger, src *GpSource, dst *GpDestination, dump *postgres.SchemaDump) error {
	if dump.IsEmpty() {
		lgr.Info("No schema objects extracted from source, skipping pre-upload")
		return nil
	}

	// SCHEMA is always required — other objects depend on it.
	types := append([]string{"SCHEMA"}, schemaMigrationPreTypes(src.AdvancedProps.SchemaMigration)...)

	dstPgSrc, err := gpCoordinatorAsPgSource(&dst.Connection)
	if err != nil {
		return xerrors.Errorf("failed to resolve destination coordinator: %w", err)
	}
	dstPool, err := postgres.MakeConnPoolFromSrc(dstPgSrc, lgr)
	if err != nil {
		return xerrors.Errorf("failed to connect to destination coordinator: %w", err)
	}
	defer dstPool.Close()

	if err := dump.ApplyDDL(ctx, lgr, dstPool, types); err != nil {
		return xerrors.Errorf("failed to apply pre-upload DDL: %w", err)
	}
	return nil
}

// transferObjectsPostUpload applies DDL that depends on data (views,
// constraints, indexes, etc.) and synchronizes sequence values.
func transferObjectsPostUpload(ctx context.Context, lgr log.Logger, src *GpSource, dst *GpDestination, dump *postgres.SchemaDump) error {
	dstPgSrc, err := gpCoordinatorAsPgSource(&dst.Connection)
	if err != nil {
		return xerrors.Errorf("failed to resolve destination coordinator: %w", err)
	}
	dstPool, err := postgres.MakeConnPoolFromSrc(dstPgSrc, lgr)
	if err != nil {
		return xerrors.Errorf("failed to connect to destination coordinator: %w", err)
	}
	defer dstPool.Close()

	postTypes := schemaMigrationPostTypes(src.AdvancedProps.SchemaMigration)
	if len(postTypes) > 0 {
		if err := dump.ApplyDDL(ctx, lgr, dstPool, postTypes); err != nil {
			return xerrors.Errorf("failed to apply post-upload DDL: %w", err)
		}
	}

	sequenceSetEnabled := src.AdvancedProps.SchemaMigration.SequenceSet == nil || *src.AdvancedProps.SchemaMigration.SequenceSet
	if sequenceSetEnabled && src.AdvancedProps.SchemaMigration.Sequence {
		srcPgSrc, err := gpCoordinatorAsPgSource(&src.Connection)
		if err != nil {
			return xerrors.Errorf("failed to resolve source coordinator: %w", err)
		}
		srcPool, err := postgres.MakeConnPoolFromSrc(srcPgSrc, lgr)
		if err != nil {
			return xerrors.Errorf("failed to connect to source coordinator for sequence values: %w", err)
		}
		defer srcPool.Close()

		if err := dump.SyncSequenceValues(ctx, lgr, srcPool, dstPool); err != nil {
			return xerrors.Errorf("failed to synchronize sequence values: %w", err)
		}
	}

	return nil
}

// dumpGpSchema builds a pg_dump connection string from GpSource and invokes pg_dump --schema-only.
func dumpGpSchema(src *GpSource) (*postgres.SchemaDump, error) {
	pgSrc, err := gpCoordinatorAsPgSource(&src.Connection)
	if err != nil {
		return nil, xerrors.Errorf("no coordinator host available: %w", err)
	}

	connString, password, err := postgres.PostgresDumpConnString(pgSrc)
	if err != nil {
		return nil, xerrors.Errorf("failed to build pg_dump connection string: %w", err)
	}

	args := []string{
		"--schema-only",
		"--no-owner",
		"--format=plain",
	}
	for _, schema := range schemasFromIncludes(src) {
		args = append(args, "-n", schema)
	}

	return postgres.NewSchemaDump(nil, connString, password, args)
}

// gpCoordinatorAsPgSource creates a minimal PgSource pointing at the GP
// coordinator so that existing postgres connection helpers (with TLS
// support) can be reused.
func gpCoordinatorAsPgSource(cfg *GpConnection) (*postgres.PgSource, error) {
	hp, err := cfg.OnPremises.Coordinator.AnyAvailable()
	if err != nil {
		return nil, err
	}
	pgs := new(postgres.PgSource)
	pgs.Hosts = []string{hp.Host}
	pgs.Port = hp.Port
	pgs.Database = cfg.Database
	pgs.User = cfg.User
	pgs.Password = cfg.AuthProps.Password
	pgs.TLSFile = cfg.AuthProps.CACertificate
	return pgs, nil
}

// schemasFromIncludes extracts schema names from GpSource.IncludeTables.
// For patterns like "schema.*" it extracts "schema".
// Returns nil if no includes are set (meaning all schemas).
func schemasFromIncludes(src *GpSource) []string {
	if len(src.IncludeTables) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	for _, table := range src.IncludeTables {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) == 2 {
			schema := strings.Trim(parts[0], "\"")
			seen[schema] = true
		}
	}
	result := make([]string, 0, len(seen))
	for schema := range seen {
		result = append(result, schema)
	}
	sort.Strings(result)
	return result
}
