package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"golang.org/x/exp/slices"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/common"
	"github.com/featureform/scheduling"
	"github.com/featureform/storage"
)

type ResourcesRepositoryType string

const (
	ResourcesRepositoryTypeMemory ResourcesRepositoryType = "memory"
	ResourcesRepositoryTypePsql   ResourcesRepositoryType = "psql"
)

const (
	sqlCountDirectDependencies = `-- name: CountDirectDependencies :one
		SELECT COUNT(*)
		FROM edges
		WHERE from_resource_proto_type = $1::integer
			AND from_resource_name = $2::text
			AND from_resource_variant = $3::text;`

	sqlMarkAsDeleted = `-- name: MarkAsDeleted :exec
		UPDATE ff_task_metadata
		SET marked_for_deletion_at = NOW()
		WHERE key = $1
		RETURNING key;`

	deleteFromParent = `-- name: DeleteFromParent :exec
		UPDATE ff_task_metadata
		SET value = (
			jsonb_build_object(
				'ResourceType', (value::jsonb ->> 'ResourceType')::int,
				'StorageType', value::jsonb ->> 'StorageType',
				'Message', jsonb_build_object(
					'name', (value::jsonb ->> 'Message')::jsonb ->> 'name',
					'defaultVariant', '',
					'variants', (
						((value::jsonb ->> 'Message')::jsonb -> 'variants') - $2
					)
				)::text,
				'SerializedVersion', (value::jsonb ->> 'SerializedVersion')::int
			)::text
		)
		WHERE key = $1`

	sqlDeleteFromEdges = `-- name: DeleteFromEdges :exec
		DELETE FROM edges
		WHERE to_resource_proto_type = $1::integer
			AND to_resource_name = $2::text
			AND to_resource_variant = $3::text;`

	getDependencies = `-- name: GetDependencies :many
		SELECT * FROM get_dependencies($1::integer, $2::text, $3::text);`

	archiveSql = `-- name: Delete :exec
		UPDATE ff_task_metadata
		SET key = concat('DELETED__', key, to_char(now(), 'YYYYMMDDTHH24MISS'))
		WHERE key = $1;`
)

type AsyncDeletionHandler func(ctx context.Context, resId ResourceID, logger logging.Logger) error

type ResourcesRepository interface {
	Archive(ctx context.Context, resourceID common.ResourceID) error
	GetDependencies(ctx context.Context, resourceID common.ResourceID) ([]common.ResourceID, error)
	MarkForDeletion(ctx context.Context, resourceID common.ResourceID, asyncDeletionHandler AsyncDeletionHandler) error
	PruneResource(ctx context.Context, resourceID common.ResourceID, asyncDeletionHandler AsyncDeletionHandler) ([]common.ResourceID, error)
	ResourceLookup
}

type SqlRepositoryConfig struct {
	MaxDelay           time.Duration
	InitialBackoff     time.Duration
	TransactionTimeout time.Duration
	Attempts           uint
}

func DefaultResourcesRepoConfig() SqlRepositoryConfig {
	return SqlRepositoryConfig{
		MaxDelay:           5 * time.Second,       // Total time we're willing to retry
		InitialBackoff:     50 * time.Millisecond, // Start with short delay
		TransactionTimeout: 10 * time.Second,      // Max time for a single transaction
		Attempts:           5,                     // Number of total attempts
	}
}

func NewResourcesRepositoryFromLookup(resourceLookup ResourceLookup) (ResourcesRepository, error) {
	if resourceLookup == nil {
		return nil, fferr.NewInternalErrorf("resource lookup is nil")
	}

	switch lookup := resourceLookup.(type) {
	case *EtcdResourceLookup, *LocalResourceLookup:
		return NewInMemoryResourcesRepository(lookup), nil

	case *MemoryResourceLookup:

		if lookup.Connection.Storage == nil {
			return nil, fferr.NewInternalErrorf("MemoryResourceLookup.Storage is nil")
		}
		switch lookup.Connection.Storage.Type() {
		case storage.MemoryMetadataStorage, storage.ETCDMetadataStorage:
			return NewInMemoryResourcesRepository(lookup), nil

		case storage.PSQLMetadataStorage:
			psqlStorage := lookup.Connection.Storage.(*storage.PSQLStorageImplementation)
			return NewSqlResourcesRepository(psqlStorage.Db, lookup, DefaultResourcesRepoConfig()), nil
		default:
			return nil, fferr.NewInternalErrorf("unsupported storage type: %v", lookup.Connection.Storage.Type())
		}

	default:
		return nil, fferr.NewInternalErrorf("unsupported resource lookup type: %T", lookup)
	}
}

type sqlResourcesRepository struct {
	db     *postgres.Pool
	config SqlRepositoryConfig
	ResourceLookup
}

func NewSqlResourcesRepository(db *postgres.Pool, lookup ResourceLookup, config SqlRepositoryConfig) ResourcesRepository {
	return &sqlResourcesRepository{
		db:             db,
		config:         config,
		ResourceLookup: lookup,
	}
}

// transactionError to combine transaction error and rollback error
type transactionError struct {
	txErr       error
	rollbackErr error
}

func (e *transactionError) Error() string {
	if e.rollbackErr != nil {
		return fmt.Sprintf("transaction error: %v, rollback error: %v", e.txErr, e.rollbackErr)
	}
	return fmt.Sprintf("transaction error: %v", e.txErr)
}

func (r *sqlResourcesRepository) withTx(ctx context.Context, logger logging.Logger, fn func(pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, r.config.TransactionTimeout)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		logger.Errorw("error starting transaction", "error", err)
		return err
	}

	txErr := fn(tx)
	if txErr != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return &transactionError{
				txErr:       txErr,
				rollbackErr: rbErr,
			}
		}
		logger.Errorw("error rolling back transaction", "error", txErr)
		return txErr
	}

	if err := tx.Commit(ctx); err != nil {
		logger.Errorw("error committing transaction", "error", err)
		return fferr.NewInternalErrorf("commit transaction: %v", err)
	}
	return nil
}

func isRetryableError(err error) bool {
	var retryablePgErrors = []string{
		"40001", // Serialization failure (error while trying to run everything in a single transaction)
	}

	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && slices.Contains(retryablePgErrors, pgErr.Code)
}

func (r *sqlResourcesRepository) Type() ResourcesRepositoryType {
	return ResourcesRepositoryTypePsql
}

func (r *sqlResourcesRepository) Archive(ctx context.Context, resourceID common.ResourceID) error {
	logger := logging.GetLoggerFromContext(ctx).
		WithResource(resourceID.Type.ToLoggingResourceType(), resourceID.Name, resourceID.Variant).
		With("function", "archive")

	err := r.withRetry(ctx, logger, func() error {
		return r.withTx(ctx, logger, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, archiveSql, resourceID.ToKey()); err != nil {
				logger.Errorw("error archiving resource", "error", err)
				return fferr.NewInternalErrorf("error archiving resource %s: %v", resourceID.ToKey(), err)
			}

			logger.Debugf("resource successfully %s archived", resourceID)
			return nil
		})
	})
	if err != nil {
		logger.Errorw("error archiving resource", "error", err)
	}
	return err
}

func (r *sqlResourcesRepository) GetDependencies(ctx context.Context, resourceID common.ResourceID) ([]common.ResourceID, error) {
	logger := logging.GetLoggerFromContext(ctx).
		WithResource(resourceID.Type.ToLoggingResourceType(), resourceID.Name, resourceID.Variant).
		With("function", "getDependencies")

	var dependencies []common.ResourceID
	err := r.withRetry(ctx, logger, func() error {
		return r.withTx(ctx, logger, func(tx pgx.Tx) error {
			var getDepsErr error
			dependencies, getDepsErr = r.getDependencies(ctx, tx, resourceID, logger)
			return getDepsErr
		})
	})
	if err != nil {
		logger.Errorw("error getting dependencies", "error", err)
		return nil, err
	}
	return dependencies, nil
}

func (r *sqlResourcesRepository) MarkForDeletion(ctx context.Context, resourceID common.ResourceID, deletionHandler AsyncDeletionHandler) error {
	logger := logging.GetLoggerFromContext(ctx).
		WithResource(resourceID.Type.ToLoggingResourceType(), resourceID.Name, resourceID.Variant).
		With("function", "MarkForDeletion")

	return r.withRetry(ctx, logger, func() error {
		return r.withTx(ctx, logger, func(tx pgx.Tx) error {
			if err := r.checkDependencies(ctx, tx, resourceID, logger); err != nil {
				logger.Errorw("error checking dependencies", "error", err)
				return err
			}

			status, err := r.getStatus(ctx, tx, resourceID, logger)
			if err != nil {
				logger.Errorw("error getting status", "error", err)
				return err
			}

			if status != scheduling.READY && status != scheduling.CREATED {
				return retry.Unrecoverable(fferr.NewInternalErrorf(
					"cannot delete resource %s %s (%s) because it is not in READY or CREATED status",
					resourceID.Type, resourceID.Name, resourceID.Variant,
				))
			}

			if err := r.markDeleted(ctx, tx, resourceID, logger); err != nil {
				logger.Errorw("error marking for deletion", "error", err)
				return err
			}

			if err := r.deleteEdges(ctx, tx, resourceID, logger); err != nil {
				logger.Errorw("error deleting edges", "error", err)
				return err
			}

			if err := r.deleteFromParent(ctx, tx, resourceID, logger); err != nil {
				logger.Errorw("error deleting from parent", "error", err)
				return err
			}

			resId := ResourceID{Name: resourceID.Name, Variant: resourceID.Variant, Type: ResourceType(resourceID.Type)}
			resource, err := r.Lookup(ctx, resId)
			if err != nil {
				logger.Errorw("error looking up resource", "error", err)
				return err
			}

			if needsJob(resource) {
				if err := deletionHandler(ctx, resId, logger); err != nil {
					logger.Errorw("error executing deletion handler", "error", err)
					return err
				}
			} else {
				if err := r.archive(ctx, tx, resourceID, logger); err != nil {
					logger.Errorw("error archiving", "error", err)
					return err
				}
			}
			logger.Debugf("resource successfully %s marked for deletion", resourceID)
			return nil
		})
	})
}

func (r *sqlResourcesRepository) PruneResource(
	ctx context.Context,
	resourceID common.ResourceID,
	asyncDeletionHandler AsyncDeletionHandler,
) ([]common.ResourceID, error) {
	var deletedResources []common.ResourceID
	logger := logging.GetLoggerFromContext(ctx)
	logger = logger.WithResource(resourceID.Type.ToLoggingResourceType(), resourceID.Name, resourceID.Variant).
		With("function", "prune")

	err := r.withRetry(ctx, logger, func() error {
		return r.withTx(ctx, logger, func(tx pgx.Tx) error {
			resId := ResourceID{Name: resourceID.Name, Variant: resourceID.Variant, Type: ResourceType(resourceID.Type)}
			_, err := r.Lookup(ctx, resId)
			if err != nil {
				logger.Errorw("error looking up resource", "error", err)
				return err
			}

			// Step 1: Get all dependencies
			logger.Debugf("Getting dependencies")
			deps, err := r.getDependencies(ctx, tx, resourceID, logger)
			if err != nil {
				logger.Errorw("error getting dependencies", "error", err)
				return err
			}

			logger.Debugw("Dependencies found", "dependencies", deps)
			// TODO We need to figure out how to handle providers as part of this process but right now we're
			// 	skipping them because they need to exist until all resources are deleted
			isProvider := resourceID.Type == common.PROVIDER
			if !isProvider {
				deps = append(deps, resourceID) // Include the resource itself if not a provider
			}

			// Clear and collect dependencies to be deleted
			deletedResources = deps

			// Step 2: Ensure all dependencies are in READY status
			logger.Debugf("Checking dependencies are ready")
			for _, dep := range deps {
				status, err := r.getStatus(ctx, tx, dep, logger)
				if err != nil {
					logger.Errorw("error getting status for dependency", "dependency", dep, "error", err)
					return err
				}
				if status != scheduling.READY {
					return retry.Unrecoverable(fferr.NewInternalErrorf(
						"cannot prune resource %s %s (%s) because dependency %s %s (%s) is not READY",
						resourceID.Type, resourceID.Name, resourceID.Variant,
						dep.Type, dep.Name, dep.Variant,
					))
				}
			}

			// Step 3: Mark all dependencies for deletion
			logger.Debugf("marking dependencies for deletion")
			for _, dep := range deps {
				if err := r.markDeleted(ctx, tx, dep, logger); err != nil {
					logger.Errorf("error marking %s for deletion: %v", dep, err)
					return err
				}
			}

			// Step 4: Delete edges associated with dependencies
			logger.Debugf("deleting edges")
			for _, dep := range deps {
				if err := r.deleteEdges(ctx, tx, dep, logger); err != nil {
					logger.Errorf("error deleting edges for %s: %v", dep, err)
					return err
				}
			}

			// Step 5: Delete from parent
			logger.Debugf("deleting from parent for")
			for _, dep := range deps {
				logger.Debugf("deleting from parent for %s", resourceID)
				if err := r.deleteFromParent(ctx, tx, dep, logger); err != nil {
					logger.Errorf("error deleting from parent for %s: %v", resourceID, err)
					return err
				}
			}

			// Step 5: Execute the async deletion handler for each resource
			for _, dep := range deps {
				resId := ResourceID{Name: dep.Name, Variant: dep.Variant, Type: ResourceType(dep.Type)}

				resource, err := r.Lookup(ctx, resId)
				if err != nil {
					logger.Errorw("error looking up resource", "error", err)
					return err
				}

				if needsJob(resource) {
					if err := asyncDeletionHandler(ctx, resId, logger); err != nil {
						logger.Errorw("error executing deletion handler", "error", err)
						return err
					}
				} else {
					if err := r.archive(ctx, tx, resId.ToCommonResourceID(), logger); err != nil {
						logger.Errorw("error hard deleting", "error", err)
						return err
					}
				}
			}

			logger.Debugf("Resource successfully pruned")
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return deletedResources, nil
}

func (r *sqlResourcesRepository) withRetry(ctx context.Context, logger logging.Logger, operation func() error) error {
	return retry.Do(
		operation,
		retry.Attempts(r.config.Attempts),    // Number of attempts
		retry.Delay(r.config.InitialBackoff), // Initial delay
		retry.MaxDelay(r.config.MaxDelay),    // Max total time
		retry.DelayType(retry.BackOffDelay),  // Exponential backoff
		retry.RetryIf(isRetryableError),      // Only retry on specific errors
		retry.OnRetry(func(n uint, err error) {
			logger.Debugw("retrying after error",
				"attempt", n+1,
				"error", err,
				"backoff_duration", time.Duration(float64(r.config.InitialBackoff)*math.Pow(2, float64(n))),
			)
		}),
		retry.Context(ctx),
	)
}

func needsJob(res Resource) bool {
	if res.ID().Type == TRAINING_SET_VARIANT ||
		res.ID().Type == SOURCE_VARIANT ||
		res.ID().Type == LABEL_VARIANT {
		return true
	}
	if res.ID().Type == FEATURE_VARIANT {
		if fv, ok := res.(*featureVariantResource); !ok {
			return false
		} else {
			return !CLIENT_COMPUTED.Equals(fv.serialized.Mode)
		}
	}
	return false
}

func (r *sqlResourcesRepository) getDependencies(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) ([]common.ResourceID, error) {
	var dependencies []common.ResourceID
	rows, err := tx.Query(ctx, getDependencies, resourceID.Type, resourceID.Name, resourceID.Variant)
	if err != nil {
		logger.Errorw("error getting dependencies", "error", err)
		return nil, err
	}
	defer rows.Close()

	// Iterate through the results
	for rows.Next() {
		var depType int
		var depName, depVariant string

		// Scan each row into local variables
		if err := rows.Scan(&depType, &depName, &depVariant); err != nil {
			logger.Errorw("error scanning dependency row", "error", err)
			return nil, fferr.NewInternalErrorf("error scanning dependency row: %v", err)
		}

		// Append the result to the dependencies slice
		dependencies = append(dependencies, common.ResourceID{
			Type:    common.ResourceType(depType),
			Name:    depName,
			Variant: depVariant,
		})
	}

	// Check for any errors during iteration
	if err := rows.Err(); err != nil {
		logger.Errorw("error iterating through dependencies", "error", err)
		return nil, fferr.NewInternalErrorf("error iterating through dependencies: %v", err)
	}

	return dependencies, nil
}

func (r *sqlResourcesRepository) checkDependencies(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) error {
	var dependencyCount int
	if err := tx.QueryRow(ctx, sqlCountDirectDependencies,
		resourceID.Type, resourceID.Name, resourceID.Variant).Scan(&dependencyCount); err != nil {
		logger.Errorw("error counting direct dependencies", "error", err)
		return fferr.NewInternalErrorf("error counting direct dependencies: %v", err)
	}

	if dependencyCount > 0 {
		return retry.Unrecoverable(fferr.NewInternalErrorf(
			"cannot delete resource %s %s (%s) because it has %d dependencies",
			resourceID.Type, resourceID.Name, resourceID.Variant, dependencyCount,
		))
	}
	return nil
}

func (r *sqlResourcesRepository) archive(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) error {
	if _, err := tx.Exec(ctx, archiveSql, resourceID.ToKey()); err != nil {
		logger.Errorw("error deleting resource", "error", err)
		return fferr.NewInternalErrorf("error deleting resource %s: %v", resourceID.ToKey(), err)
	}
	return nil
}

func (r *sqlResourcesRepository) getStatus(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) (scheduling.Status, error) {
	// 1. Attempt to get the latest task status first
	const taskStatusQuery = `
			WITH task_info AS (
		-- Step 1: Extract the first task ID from taskIdList
		SELECT (
				   SELECT (jsonb_array_elements_text((ff.value::jsonb ->> 'Message')::jsonb -> 'taskIdList'))::INTEGER
				   LIMIT 1
			   ) AS task_id
		FROM ff_task_metadata ff
		WHERE ff.key = $1
		LIMIT 1
	),
	
	task_runs AS (
		-- Step 2: Get all task runs for the extracted task_id
		SELECT tr.value::jsonb AS runs_data, ti.task_id
		FROM task_info ti
		JOIN ff_task_metadata tr
			 ON tr.key = '/tasks/runs/task_id=' || ti.task_id
		WHERE ti.task_id IS NOT NULL
	),
	
	latest_run AS (
		-- Step 3: Get the latest run based on dateCreated
		SELECT run_obj->>'runID' AS run_id,
			   run_obj->>'dateCreated' AS date_created,
			   tr.task_id
		FROM task_runs tr,
			 jsonb_array_elements(tr.runs_data -> 'runs') AS run_obj
		ORDER BY run_obj->>'dateCreated' DESC
		LIMIT 1
	)
	
	-- Step 4: Get the status directly from the run metadata
	SELECT (run_metadata.value::jsonb ->> 'status')::INTEGER AS status
	FROM latest_run lr
	JOIN ff_task_metadata run_metadata
		 ON run_metadata.key = '/tasks/runs/metadata/' ||
							   to_char((lr.date_created)::timestamptz, 'YYYY/MM/DD/HH24/MI') ||
							   '/task_id=' || lr.task_id ||
							   '/run_id=' || lr.run_id
	`

	var taskStatus sql.NullInt32

	// 2. Try to get the task status
	err := tx.QueryRow(ctx, taskStatusQuery, resourceID.ToKey()).Scan(&taskStatus)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		logger.Errorw("error getting task status", "error", err)
		return scheduling.NO_STATUS, fferr.NewInternalErrorf("failed to get task status for resource %s: %v", resourceID.ToKey(), err)
	}

	// 3. If task status is found, return it
	if taskStatus.Valid {
		return scheduling.Status(taskStatus.Int32), nil
	}

	// 4. Fallback: Get direct status from the resource
	const directStatusQuery = `
		SELECT ((ff.value::jsonb ->> 'Message')::jsonb -> 'status' ->> 'status')
		FROM ff_task_metadata ff
		WHERE ff.key = $1
		LIMIT 1;
	`

	var directStatus sql.NullString

	err = tx.QueryRow(ctx, directStatusQuery, resourceID.ToKey()).Scan(&directStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return scheduling.NO_STATUS, nil
		}
		logger.Errorw("error getting direct status", "error", err)
		return scheduling.NO_STATUS, fferr.NewInternalErrorf("failed to get direct status for resource %s: %v", resourceID.ToKey(), err)
	}

	if directStatus.Valid {
		return scheduling.ParseStatus(directStatus.String), nil
	}

	return scheduling.NO_STATUS, nil
}

func (r *sqlResourcesRepository) markDeleted(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) error {
	var deletedKey string
	if err := tx.QueryRow(ctx, sqlMarkAsDeleted, resourceID.ToKey()).Scan(&deletedKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return retry.Unrecoverable(fferr.NewInternalErrorf(
				"resource with key %s does not exist in ff_task_metadata",
				resourceID.ToKey(),
			))
		}
		logger.Errorf("error marking as deleted %s: %v", resourceID.ToKey(), err)
		return fferr.NewInternalErrorf("mark as deleted %s: %v", resourceID.ToKey(), err)
	}
	return nil
}

func (r *sqlResourcesRepository) deleteEdges(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) error {
	if _, err := tx.Exec(ctx, sqlDeleteFromEdges, resourceID.Type, resourceID.Name, resourceID.Variant); err != nil {
		logger.Errorw("error deleting edges", "error", err)
		return fferr.NewInternalErrorf("error deleting edges for resource %s: %v", resourceID, err)
	}
	return nil
}

// add a delete from parent
func (r *sqlResourcesRepository) deleteFromParent(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID, logger logging.Logger) error {
	parent, hasParent := resourceID.Parent()
	if hasParent {
		if _, err := tx.Exec(ctx, deleteFromParent, parent.ToKey(), resourceID.Variant); err != nil {
			logger.Errorw("error deleting from parent", "parent", parent, "resource", resourceID, "error", err)
			return fferr.NewInternalErrorf("could not delete from parent %s: %v", parent, err)
		}
	}
	return nil
}

type inMemoryResourcesRepository struct {
	ResourceLookup
}

func NewInMemoryResourcesRepository(lookup ResourceLookup) ResourcesRepository {
	return &inMemoryResourcesRepository{ResourceLookup: lookup}
}

func (r *inMemoryResourcesRepository) MarkForDeletion(ctx context.Context, resourceID common.ResourceID, deletionHandler AsyncDeletionHandler) error {
	return fferr.NewInternalErrorf("mark for deletion not supported in memory")
}

func (r *inMemoryResourcesRepository) Type() ResourcesRepositoryType {
	return ResourcesRepositoryTypeMemory
}

func (r *inMemoryResourcesRepository) PruneResource(ctx context.Context, resourceID common.ResourceID, deletionHandler AsyncDeletionHandler) ([]common.ResourceID, error) {
	return nil, fferr.NewInternalErrorf("prune resource not supported in memory")
}

func (r *inMemoryResourcesRepository) GetDependencies(ctx context.Context, resourceID common.ResourceID) ([]common.ResourceID, error) {
	return nil, fferr.NewInternalErrorf("get dependencies not supported in memory")
}

func (r *inMemoryResourcesRepository) Archive(ctx context.Context, resourceID common.ResourceID) error {
	return fferr.NewInternalErrorf("archive not supported in memory")
}
