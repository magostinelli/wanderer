package util

// The matcher recognizes all candidate actor identifiers in a payload in one
// pass. It is a byte-oriented Aho-Corasick automaton: ActivityPub IRIs and
// PocketBase IDs are compared byte-for-byte, matching SQLite instr semantics.
type actorReferenceMask uint8

const (
	actorReferenceActivity actorReferenceMask = 1 << iota
	actorReferenceNotification
)

type actorReferenceHit struct {
	actor int
	mask  actorReferenceMask
}

type actorReferenceNode struct {
	next    map[byte]int
	failure int
	outputs []actorReferenceHit
}

type actorReferenceMatcher struct {
	nodes []actorReferenceNode
}

func newActorReferenceMatcher() *actorReferenceMatcher {
	return &actorReferenceMatcher{
		nodes: []actorReferenceNode{{}},
	}
}

func (m *actorReferenceMatcher) add(pattern string, actor int, mask actorReferenceMask) {
	if pattern == "" || mask == 0 {
		return
	}
	state := 0
	for i := 0; i < len(pattern); i++ {
		char := pattern[i]
		next, ok := m.nodes[state].next[char]
		if !ok {
			next = len(m.nodes)
			if m.nodes[state].next == nil {
				m.nodes[state].next = make(map[byte]int)
			}
			m.nodes[state].next[char] = next
			m.nodes = append(m.nodes, actorReferenceNode{})
		}
		state = next
	}
	for i := range m.nodes[state].outputs {
		if m.nodes[state].outputs[i].actor == actor {
			m.nodes[state].outputs[i].mask |= mask
			return
		}
	}
	m.nodes[state].outputs = append(m.nodes[state].outputs, actorReferenceHit{
		actor: actor,
		mask:  mask,
	})
}

// build computes failure transitions and inherits suffix matches. It must be
// called after all patterns have been added and before mark is used.
func (m *actorReferenceMatcher) build() {
	queue := make([]int, 0, len(m.nodes))
	for _, child := range m.nodes[0].next {
		m.nodes[child].failure = 0
		queue = append(queue, child)
	}

	for head := 0; head < len(queue); head++ {
		state := queue[head]
		for char, child := range m.nodes[state].next {
			failure := m.nodes[state].failure
			for failure != 0 {
				if _, ok := m.nodes[failure].next[char]; ok {
					break
				}
				failure = m.nodes[failure].failure
			}
			if next, ok := m.nodes[failure].next[char]; ok && next != child {
				m.nodes[child].failure = next
			} else {
				m.nodes[child].failure = 0
			}
			failureOutputs := m.nodes[m.nodes[child].failure].outputs
			if len(failureOutputs) > 0 {
				m.nodes[child].outputs = append(m.nodes[child].outputs, failureOutputs...)
			}
			queue = append(queue, child)
		}
	}
}

// mark sets every actor whose pattern occurs in text. excludedIRI suppresses
// all matches for the activity's actor are ignored, mirroring the SQL actor
// inequality. kind keeps notification-only handles out
// of ActivityPub activity matching.
func (m *actorReferenceMatcher) mark(
	text string,
	excludedIRI string,
	kind actorReferenceMask,
	candidates []RemoteActorCandidate,
	referenced []bool,
) {
	state := 0
	for i := 0; i < len(text); i++ {
		char := text[i]
		for state != 0 {
			if _, ok := m.nodes[state].next[char]; ok {
				break
			}
			state = m.nodes[state].failure
		}
		if next, ok := m.nodes[state].next[char]; ok {
			state = next
		}
		for _, hit := range m.nodes[state].outputs {
			if hit.mask&kind == 0 || referenced[hit.actor] {
				continue
			}
			if excludedIRI != "" && candidates[hit.actor].IRI == excludedIRI {
				continue
			}
			referenced[hit.actor] = true
		}
	}
}
