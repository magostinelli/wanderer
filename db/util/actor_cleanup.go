package util

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// RemoteActorReferenceCount checks every PocketBase relation field that points
// to activitypub_actors, then checks the non-relation ActivityPub payloads
// that store actor identities as IRIs and notification metadata that stores
// handles. Unknown/query errors are returned and must block deletion.
func RemoteActorReferenceCount(app core.App, actor *core.Record) (int64, error) {
	if actor == nil || actor.GetBool("is_local") {
		return 0, errors.New("expected a remote actor")
	}

	collections, err := app.FindAllCollections()
	if err != nil {
		return 0, fmt.Errorf("list collections: %w", err)
	}

	total := int64(0)
	for _, collection := range collections {
		// Views are derived from base records and do not own independent
		// references. Counting them would double-count relation fields.
		if collection.IsView() {
			continue
		}
		for _, field := range collection.Fields {
			relation, ok := field.(*core.RelationField)
			if !ok || relation.CollectionId != actor.Collection().Id {
				continue
			}
			// These author relations are handled by DeleteRemoteActor, which
			// permits remote copies but refuses records with local identities.
			if field.GetName() == "author" && isRemoteCopyCollection(collection.Name) {
				continue
			}

			var count int64
			if relation.IsMultiple() {
				// Multi-select relations are stored as JSON arrays in SQLite.
				column := "\"" + strings.ReplaceAll(field.GetName(), "\"", "\"\"") + "\""
				count, err = app.CountRecords(collection.Name, dbx.NewExp(
					"EXISTS (SELECT 1 FROM json_each("+column+") WHERE value={:id})",
					dbx.Params{"id": actor.Id},
				))
			} else {
				count, err = app.CountRecords(collection.Name, dbx.HashExp{field.GetName(): actor.Id})
			}
			if err != nil {
				return 0, fmt.Errorf("count %s.%s references: %w", collection.Name, field.GetName(), err)
			}
			total += count
		}
	}

	iri := actor.GetString("iri")
	if iri == "" {
		return 0, errors.New("remote actor has no IRI")
	}

	// Activity records use actor IRIs rather than PocketBase relation IDs.
	// Search all fields that can carry the actor or its audience/object links.
	activityCount, err := app.CountRecords("activitypub_activities", dbx.NewExp(
		"COALESCE(actor, '') != {:iri} AND (instr(COALESCE(object, ''), {:iri}) > 0 OR instr(COALESCE(cc, ''), {:iri}) > 0 OR instr(COALESCE("to", ''), {:iri}) > 0 OR instr(COALESCE(relation, ''), {:iri}) > 0 OR instr(COALESCE(object, ''), {:id}) > 0 OR instr(COALESCE(cc, ''), {:id}) > 0 OR instr(COALESCE("to", ''), {:id}) > 0 OR instr(COALESCE(relation, ''), {:id}) > 0)",
		dbx.Params{"iri": iri, "id": actor.Id},
	))
	if err != nil {
		return 0, fmt.Errorf("count ActivityPub activity references: %w", err)
	}
	total += activityCount

	// Notifications currently store a human-readable handle in metadata.
	// This is deliberately a substring match: a false positive is safer than
	// removing an actor whose handle is still embedded in a notification.
	username, domain := actor.GetString("preferred_username"), actor.GetString("domain")
	metadataFilter := "instr(COALESCE(metadata, ''), {:iri}) > 0 OR instr(COALESCE(metadata, ''), {:id}) > 0"
	metadataParams := dbx.Params{"iri": iri, "id": actor.Id}
	if username != "" && domain != "" {
		metadataFilter += " OR instr(COALESCE(metadata, ''), {:handle}) > 0"
		metadataParams["handle"] = "@" + username + "@" + domain
	}
	notificationCount, err := app.CountRecords("notifications", dbx.NewExp(metadataFilter, metadataParams))
	if err != nil {
		return 0, fmt.Errorf("count notification metadata references: %w", err)
	}
	total += notificationCount

	return total, nil
}

// DeleteUnreferencedRemoteActor deletes only a remote actor with no known
// references. The check and deletion share a transaction. Normal PocketBase
// delete hooks remain enabled: they purge the actor's local ActivityPub
// activities and remove the Meilisearch document. The federation delete hook
// is safe for remote actors: CreateActorDeleteActivity explicitly returns
// without creating or delivering a Delete activity when is_local is false.
func DeleteUnreferencedRemoteActor(app core.App, actor *core.Record) error {
	if actor == nil || actor.GetBool("is_local") {
		return errors.New("refusing to delete a local or nil actor")
	}

	return app.RunInTransaction(func(txApp core.App) error {
		// Re-fetch inside the transaction to avoid deleting a stale record.
		current, err := txApp.FindRecordById("activitypub_actors", actor.Id)
		if err != nil {
			return fmt.Errorf("reload actor %s: %w", actor.Id, err)
		}
		if current.GetBool("is_local") || current.GetString("iri") != actor.GetString("iri") {
			return errors.New("actor identity changed or actor is local")
		}

		count, err := RemoteActorReferenceCount(txApp, current)
		if err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("actor %s still has %d reference(s)", current.Id, count)
		}

		// Reuse the project's established remote-copy validation and record
		// deletion path. It rejects records carrying local identities.
		return deleteRemoteActorInTransaction(txApp, current)
	})
}



 
// RemoteActorCandidate is a remote actor that the reference scan found unused.
type RemoteActorCandidate struct {
	ID                 string
	IRI                string
	PreferredUsername  string
	Domain             string
}

// StreamUnreferencedRemoteActors evaluates the full reference graph in one
// SQLite query and streams unreferenced actors in stable ID order. This avoids
// issuing one query per actor on installations with very large actor caches.
// The callback may stop the scan by returning an error.
func StreamUnreferencedRemoteActors(app core.App, afterID string, visit func(RemoteActorCandidate) error) error {
	if visit == nil {
		return errors.New("missing actor candidate visitor")
	}

	actorCollection, err := app.FindCachedCollectionByNameOrId("activitypub_actors")
	if err != nil {
		return fmt.Errorf("find actor collection: %w", err)
	}
	collections, err := app.FindAllCollections()
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}

	references := make([]string, 0)
	for _, collection := range collections {
		if collection.IsView() {
			continue
		}
		for _, field := range collection.Fields {
			relation, ok := field.(*core.RelationField)
			if !ok || relation.CollectionId != actorCollection.Id {
				continue
			}
			if field.GetName() == "author" && isRemoteCopyCollection(collection.Name) {
				continue
			}

			table := quoteIdentifier(collection.Name)
			column := quoteIdentifier(field.GetName())
			if relation.IsMultiple() {
				references = append(references,
					"SELECT relation_values.value AS actor_id FROM "+table+
						", json_each("+column+") AS relation_values WHERE relation_values.value <> ''")
			} else {
				references = append(references,
					"SELECT "+column+" AS actor_id FROM "+table+" WHERE "+column+" <> ''")
			}
		}
	}

	references = append(references,
		`SELECT a.id AS actor_id
		 FROM activitypub_actors a
		 JOIN activitypub_activities x
		   ON COALESCE(x.actor, '') <> a.iri
		  AND (instr(COALESCE(x.object, ''), a.iri) > 0
		    OR instr(COALESCE(x.cc, ''), a.iri) > 0
		    OR instr(COALESCE(x."to", ''), a.iri) > 0
		    OR instr(COALESCE(x.relation, ''), a.iri) > 0
		    OR instr(COALESCE(x.object, ''), a.id) > 0
		    OR instr(COALESCE(x.cc, ''), a.id) > 0
		    OR instr(COALESCE(x."to", ''), a.id) > 0
		    OR instr(COALESCE(x.relation, ''), a.id) > 0)
		 WHERE a.is_local = 0 AND a.iri <> ''`,
		`SELECT a.id AS actor_id
		 FROM activitypub_actors a
		 JOIN notifications n
		   ON (instr(COALESCE(n.metadata, ''), a.iri) > 0
		    OR instr(COALESCE(n.metadata, ''), a.id) > 0
		    OR (a.preferred_username <> '' AND a.domain <> ''
		        AND instr(COALESCE(n.metadata, ''), '@' || a.preferred_username || '@' || a.domain) > 0))
		 WHERE a.is_local = 0 AND a.iri <> ''`,
	)

	query := "WITH refs(actor_id) AS (" + strings.Join(references, " UNION ALL ") + `) 
		SELECT a.id, a.iri, a.preferred_username, a.domain
		FROM activitypub_actors a
		WHERE a.is_local = 0 AND a.iri <> '' AND a.id > {:after}
		  AND NOT EXISTS (SELECT 1 FROM refs r WHERE r.actor_id = a.id)
		ORDER BY a.id`

	rows, err := app.DB().NewQuery(query).Bind(dbx.Params{"after": afterID}).Rows()
	if err != nil {
		return fmt.Errorf("query unreferenced remote actors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var candidate RemoteActorCandidate
		if err := rows.Scan(&candidate.ID, &candidate.IRI, &candidate.PreferredUsername, &candidate.Domain); err != nil {
			return fmt.Errorf("scan unreferenced remote actor: %w", err)
		}
		if err := visit(candidate); err != nil {
			return err
		}
	}
	return rows.Err()
}

func quoteIdentifier(value string) string {
	return "\"" + strings.ReplaceAll(value, "\"", "\"\"") + "\""
}
// isRemoteCopyCollection identifies collections whose author records are
// already validated by DeleteRemoteActor before the actor is removed.
func isRemoteCopyCollection(name string) bool {
	switch name {
	case "trails", "lists", "comments", "summit_logs", "waypoints":
		return true
	default:
		return false
	}
}
