package util

import "testing"

func TestActorReferenceMatcherMatchesPayloadKindsAndExcludesOwnActivity(t *testing.T) {
	candidates := []RemoteActorCandidate{
		{ID: "actor0001", IRI: "https://remote.example/users/alice", PreferredUsername: "alice", Domain: "remote.example"},
		{ID: "actor0002", IRI: "https://remote.example/users/bob", PreferredUsername: "bob", Domain: "remote.example"},
	}
	matcher := newActorReferenceMatcher()
	for index, candidate := range candidates {
		matcher.add(candidate.IRI, index, actorReferenceActivity|actorReferenceNotification)
		matcher.add(candidate.ID, index, actorReferenceActivity|actorReferenceNotification)
		matcher.add("@"+candidate.PreferredUsername+"@"+candidate.Domain, index, actorReferenceNotification)
	}
	matcher.build()

	referenced := make([]bool, len(candidates))
	// The activity is authored by Alice, so all Alice matches in this row are
	// ignored. Bob's IRI remains a valid reference.
	matcher.mark(candidates[0].IRI+" mentions "+candidates[1].IRI,
		candidates[0].IRI, actorReferenceActivity, candidates, referenced)
	if referenced[0] || !referenced[1] {
		t.Fatalf("unexpected activity matches: %v", referenced)
	}

	referenced = make([]bool, len(candidates))
	// Handles are notification-only, not ActivityPub activity references.
	matcher.mark("mention @alice@remote.example", "",
		actorReferenceActivity, candidates, referenced)
	if referenced[0] || referenced[1] {
		t.Fatalf("notification handle matched as activity: %v", referenced)
	}

	matcher.mark(`{"handle":"@alice@remote.example"}`, "",
		actorReferenceNotification, candidates, referenced)
	if !referenced[0] {
		t.Fatalf("notification handle was not matched: %v", referenced)
	}
}
