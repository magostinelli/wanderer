package main

import (
	"testing"
	"time"

	"pocketbase/util"
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

func TestRemoteActorCleanupPreservesActorReferencedByFollow(t *testing.T) {
	app := newRealSchemaApp(t)
	actor := saveInboundRecord(t, app, "activitypub_actors", map[string]any{
		"username": "Remote", "preferred_username": "remote", "domain": "remote.example",
		"iri": "https://remote.example/ap/actors/remote", "inbox": "https://remote.example/inbox", "public_key": "remote-key",
	})
	saveInboundRecord(t, app, "follows", map[string]any{"follower": actor.Id})

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
	saveInboundRecord(t, app, "follows", map[string]any{"follower": referenced.Id})

	var candidates []util.RemoteActorCandidate
	err := util.StreamUnreferencedRemoteActors(app, "", func(candidate util.RemoteActorCandidate) error {
		candidates = append(candidates, candidate)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ID != unused.Id {
		t.Fatalf("unexpected cleanup candidates: %+v", candidates)
	}
}
