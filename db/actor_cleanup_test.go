package main

import (
	"testing"
	"time"

	"pocketbase/util"

	"github.com/pocketbase/pocketbase/core"
)

func TestRemoteActorCleanupDeletesOnlyUnreferencedCacheActor(t *testing.T) {
	app := newRealSchemaApp(t)
	actor := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Remote", "preferred_username": "remote", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/remote", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})

	// A remote copy authored by the actor is allowed: DeleteRemoteActor checks
	// that its IRI is not on this instance's own path before cascading it.
	copy := saveInboundRecord(t, app, "trails", map[string]any{
		"author": actor.Id, "name": "Remote copy",
		"iri": "https://remote.example/api/v1/trail/remotecopy00001",
	})

	count, err := util.RemoteActorReferenceCount(app, actor)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("remote authored copy should not count as an external reference, got %d", count)
	}

	if err := util.DeleteUnreferencedRemoteActor(app, actor); err != nil {
		t.Fatal(err)
	}
	if _, err := app.FindRecordById("activitypub_actors", actor.Id); err == nil {
		t.Fatal("actor was not deleted")
	}
	if _, err := app.FindRecordById("trails", copy.Id); err == nil {
		t.Fatal("remote copy was not cascaded with its actor")
	}
}

func TestRemoteActorCleanupPreservesActorReferencedByRelation(t *testing.T) {
	app := newRealSchemaApp(t)
	actor := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Remote", "preferred_username": "remote", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/remote", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	follower := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Follower", "preferred_username": "follower", "domain": "other.example",
		"iri": "https://other.example/ap/actors/follower", "inbox": "https://other.example/inbox", "public_key": "follower-key",
	})
	saveActorRelationReference(t, app, follower, actor)

	count, err := util.RemoteActorReferenceCount(app, actor)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("follow relation was not detected")
	}
	if err := util.DeleteUnreferencedRemoteActor(app, actor); err == nil {
		t.Fatal("referenced actor deletion unexpectedly succeeded")
	}
	if _, err := app.FindRecordById("activitypub_actors", actor.Id); err != nil {
		t.Fatalf("referenced actor was removed: %v", err)
	}
}

func TestRemoteActorCleanupRefusesLocalIdentityRecords(t *testing.T) {
	app := newRealSchemaApp(t)
	actor := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Remote", "preferred_username": "remote", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/remote", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	trail := saveInboundRecord(t, app, "trails", map[string]any{
		"author": actor.Id, "name": "Unexpected local identity",
	})
	trail.Set("iri", "https://example.com/api/v1/trail/"+trail.Id)
	if err := app.SaveNoValidate(trail); err != nil {
		t.Fatal(err)
	}

	if err := util.DeleteUnreferencedRemoteActor(app, actor); err == nil {
		t.Fatal("actor with a local-identity record was deleted")
	}
	if _, err := app.FindRecordById("activitypub_actors", actor.Id); err != nil {
		t.Fatalf("actor was removed: %v", err)
	}
	if _, err := app.FindRecordById("trails", trail.Id); err != nil {
		t.Fatalf("local-identity trail was removed: %v", err)
	}
}

func TestRemoteActorCleanupDistinguishesOwnedAndForeignActivities(t *testing.T) {
	app := newRealSchemaApp(t)
	actor := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Remote", "preferred_username": "remote", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/remote", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	iri := actor.GetString("iri")

	saveInboundRecord(t, app, "activitypub_activities", map[string]any{
		"iri": "https://remote.example/activities/owned", "type": "Create",
		"object": iri, "actor": iri, "published": time.Now(),
	})
	count, err := util.RemoteActorReferenceCount(app, actor)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("actor's own outbox activity should be purged with the actor, got %d references", count)
	}

	saveInboundRecord(t, app, "activitypub_activities", map[string]any{
		"iri": "https://another.example/activities/mention", "type": "Create",
		"object": iri, "actor": "https://another.example/actors/alice", "published": time.Now(),
	})
	count, err = util.RemoteActorReferenceCount(app, actor)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("foreign activity referencing the actor was not detected")
	}
}

func TestStreamUnreferencedRemoteActorsUsesReferenceGraph(t *testing.T) {
	app := newRealSchemaApp(t)
	unused := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Unused", "preferred_username": "unused", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/unused", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	referenced := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Followed", "preferred_username": "followed", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/followed", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	follower := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Follower", "preferred_username": "follower", "domain": "other.example",
		"iri": "https://other.example/ap/actors/follower", "inbox": "https://other.example/inbox", "public_key": "follower-key",
	})
	saveActorRelationReference(t, app, follower, referenced)

	// An ActivityPub payload reference must also exclude an actor even when
	// there is no PocketBase relation pointing to it.
	activityOnly := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "ActivityOnly", "preferred_username": "activityonly", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/activityonly", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	saveInboundRecord(t, app, "activitypub_activities", map[string]any{
		"iri": "https://another.example/activities/mention", "type": "Create",
		"object": activityOnly.GetString("iri"), "actor": "https://another.example/actors/alice",
		"published": time.Now(),
	})

	// Notification metadata can refer to an actor by its human-readable handle.
	notificationOnly := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "NotificationOnly", "preferred_username": "notificationonly", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/notificationonly", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	// Notification author and recipient are ActivityPub actor relations in
	// the current schema. Use an unrelated actor for both fields so the only
	// reference to notificationOnly is the handle in metadata.
	saveInboundRecord(t, app, "notifications", map[string]any{
		"author": follower.Id, "recipient": follower.Id, "type": "trail_comment",
		"metadata": `{"actor":"@notificationonly@remote.example"}`,
	})

	// An actor's own activity is removed with its actor and must not keep it
	// alive merely because its own IRI appears in its object payload.
	ownActivity := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "OwnActivity", "preferred_username": "ownactivity", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/ownactivity", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	saveInboundRecord(t, app, "activitypub_activities", map[string]any{
		"iri": "https://remote.example/activities/own", "type": "Create",
		"object": ownActivity.GetString("iri"), "actor": ownActivity.GetString("iri"),
		"published": time.Now(),
	})

	var candidates []util.RemoteActorCandidate
	err = util.StreamUnreferencedRemoteActors(app, "", func(candidate util.RemoteActorCandidate) error {
		candidates = append(candidates, candidate)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{unused.Id: true, ownActivity.Id: true}
	got := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		got[candidate.ID] = true
	}
	if len(got) != len(want) || !got[unused.Id] || !got[ownActivity.Id] {
		t.Fatalf("unexpected cleanup candidates (activity-only actor %s and notification-only actor %s must be excluded): %+v", activityOnly.Id, notificationOnly.Id, candidates)
	}
}


// saveActorRelationReference adds a relation field to trails for the test.
// The production follows collection relates PocketBase users, not ActivityPub
// actor records, so it cannot represent an actor-to-actor reference.
func saveActorRelationReference(t *testing.T, app core.App, follower, followee *core.Record) {
	t.Helper()
	trails, err := app.FindCollectionByNameOrId("trails")
	if err != nil {
		t.Fatal(err)
	}
	trails.Fields.Add(&core.RelationField{
		Name:         "cleanup_reference",
		CollectionId: followee.Collection().Id,
		MaxSelect:    1,
	})
	if err := app.Save(trails); err != nil {
		t.Fatal(err)
	}
	saveInboundRecord(t, app, "trails", map[string]any{
		"author": follower.Id, "cleanup_reference": followee.Id, "name": "Actor relation reference",
	})
}
