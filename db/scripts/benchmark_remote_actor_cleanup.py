#!/usr/bin/env python3
"""Synthetic SQLite benchmark for remote ActivityPub actor cleanup queries.

The fixture is generated in memory. This script never opens PocketBase data,
contacts Meilisearch, or uses the network. Results compare query shapes and
correctness; they are not a production performance estimate.
"""
from __future__ import annotations

import argparse
import random
import sqlite3
import statistics
import time

PROFILES = {"sparse": 0.01, "medium": 0.10, "dense": 0.40}
SEED = 20261103
INSERT_CHUNK = 10_000


def chunks(iterable, size):
    batch = []
    for row in iterable:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def make_db(actor_count, activity_count, notification_count, profile, seed):
    started = time.perf_counter()
    rng = random.Random(seed)
    db = sqlite3.connect(":memory:")
    db.execute("PRAGMA temp_store = MEMORY")
    db.executescript("""
        CREATE TABLE activitypub_actors (
            id TEXT PRIMARY KEY, iri TEXT NOT NULL,
            preferred_username TEXT NOT NULL, domain TEXT NOT NULL,
            is_local INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE activitypub_activities (
            id INTEGER PRIMARY KEY, actor TEXT, object TEXT,
            cc TEXT, "to" TEXT, relation TEXT
        );
        CREATE TABLE notifications (id INTEGER PRIMARY KEY, metadata TEXT);
        CREATE TABLE relation_refs (actor_id TEXT NOT NULL);
        CREATE INDEX idx_relation_refs_actor_id ON relation_refs(actor_id);
    """)

    actors = [
        (f"actor{i:09d}", f"https://remote{i}.example/users/user{i}",
         f"user{i}", f"remote{i}.example", 1 if i % 100 == 0 else 0)
        for i in range(actor_count)
    ]
    db.executemany(
        "INSERT INTO activitypub_actors VALUES (?, ?, ?, ?, ?)",
        actors,
    )
    # Half of remote actors are already referenced by ordinary relations.
    db.executemany(
        "INSERT INTO relation_refs(actor_id) VALUES (?)",
        ((actors[i][0],) for i in range(0, actor_count, 2) if actors[i][4] == 0),
    )

    eligible = [a for i, a in enumerate(actors) if a[4] == 0 and i % 2 == 1]
    coverage = PROFILES[profile]
    target_count = min(len(eligible), round(len(eligible) * coverage))
    payload_targets = rng.sample(eligible, target_count) if target_count else []
    payload_targets = payload_targets or []
    # A fixed fraction of candidate actors is referenced by payloads. Spread
    # those references across all four searchable activity columns and metadata.
    activity_targets = payload_targets[:]
    notification_targets = payload_targets[:]
    rng.shuffle(activity_targets)
    rng.shuffle(notification_targets)

    def activity_rows():
        fields = ("object", "cc", "to", "relation")
        for i in range(activity_count):
            target = activity_targets[i % len(activity_targets)] if activity_targets and i < len(activity_targets) else None
            # Additional random references make payload density scale with volume.
            if activity_targets and (target is None) and rng.random() < 0.10:
                target = rng.choice(activity_targets)
            values = ["", "", "", ""]
            if target:
                field = fields[i % len(fields)]
                values[fields.index(field)] = (
                    target[1] if i % 3 else target[0]
                )
            else:
                values[i % 4] = f"https://unrelated.example/objects/{i}"
            yield (i, "https://local.example/users/owner", *values)

    db.executemany(
        'INSERT INTO activitypub_activities(id, actor, object, cc, "to", relation) VALUES (?, ?, ?, ?, ?, ?)',
        (row for block in chunks(activity_rows(), INSERT_CHUNK) for row in block),
    )

    def notification_rows():
        for i in range(notification_count):
            target = notification_targets[i] if i < len(notification_targets) else None
            if target is None and notification_targets and rng.random() < 0.05:
                target = rng.choice(notification_targets)
            if target:
                metadata = (
                    f'{{"actor":"{target[1]}","handle":"@{target[2]}@{target[3]}"}}'
                    if i % 2 else f'{{"actor_id":"{target[0]}"}}'
                )
            else:
                metadata = f'{{"message":"notification {i} unrelated payload"}}'
            yield (i, metadata)

    db.executemany(
        "INSERT INTO notifications(id, metadata) VALUES (?, ?)",
        (row for block in chunks(notification_rows(), INSERT_CHUNK) for row in block),
    )
    db.commit()
    page_count = db.execute("PRAGMA page_count").fetchone()[0]
    page_size = db.execute("PRAGMA page_size").fetchone()[0]
    return db, time.perf_counter() - started, page_count * page_size, len(eligible), target_count


PAYLOAD_CTES = """
payload_refs(actor_id) AS (
    SELECT ca.id
    FROM candidate_actors ca
    JOIN activitypub_activities x
      ON COALESCE(x.actor, '') <> ca.iri
     AND (instr(COALESCE(x.object, ''), ca.iri) > 0
       OR instr(COALESCE(x.cc, ''), ca.iri) > 0
       OR instr(COALESCE(x."to", ''), ca.iri) > 0
       OR instr(COALESCE(x.relation, ''), ca.iri) > 0
       OR instr(COALESCE(x.object, ''), ca.id) > 0
       OR instr(COALESCE(x.cc, ''), ca.id) > 0
       OR instr(COALESCE(x."to", ''), ca.id) > 0
       OR instr(COALESCE(x.relation, ''), ca.id) > 0)
    UNION ALL
    SELECT ca.id
    FROM candidate_actors ca
    JOIN notifications n
      ON (instr(COALESCE(n.metadata, ''), ca.iri) > 0
       OR instr(COALESCE(n.metadata, ''), ca.id) > 0
       OR (ca.preferred_username <> '' AND ca.domain <> ''
           AND instr(COALESCE(n.metadata, ''), '@' || ca.preferred_username || '@' || ca.domain) > 0))
)
"""


def one_pass(db):
    query = """
    WITH candidate_actors AS MATERIALIZED (
        SELECT a.id, a.iri, a.preferred_username, a.domain
        FROM activitypub_actors a
        WHERE a.is_local = 0 AND a.iri <> ''
          AND NOT EXISTS (SELECT 1 FROM relation_refs r WHERE r.actor_id = a.id)
    ),
    """ + PAYLOAD_CTES + """
    SELECT ca.id FROM candidate_actors ca
    WHERE NOT EXISTS (SELECT 1 FROM payload_refs p WHERE p.actor_id = ca.id)
    ORDER BY ca.id
    """
    return [row[0] for row in db.execute(query)]


def paged(db, batch_size):
    results = []
    after = ""
    while True:
        batch = db.execute("""
            SELECT id, iri, preferred_username, domain
            FROM activitypub_actors
            WHERE is_local = 0 AND iri <> '' AND id > ?
            ORDER BY id LIMIT ?
        """, (after, batch_size)).fetchall()
        if not batch:
            break
        after = batch[-1][0]
        db.execute("DROP TABLE IF EXISTS temp.actor_batch")
        db.execute("""
            CREATE TEMP TABLE actor_batch (
                id TEXT PRIMARY KEY, iri TEXT,
                preferred_username TEXT, domain TEXT
            )
        """)
        db.executemany(
            "INSERT INTO actor_batch VALUES (?, ?, ?, ?)", batch
        )
        query = """
        WITH candidate_actors AS MATERIALIZED (
            SELECT a.id, a.iri, a.preferred_username, a.domain
            FROM actor_batch a
            WHERE NOT EXISTS (SELECT 1 FROM relation_refs r WHERE r.actor_id = a.id)
        ),
        """ + PAYLOAD_CTES + """
        SELECT ca.id FROM candidate_actors ca
        WHERE NOT EXISTS (SELECT 1 FROM payload_refs p WHERE p.actor_id = ca.id)
        ORDER BY ca.id
        """
        results.extend(row[0] for row in db.execute(query))
    return results


def timed(fn, repeats):
    samples, output = [], []
    for _ in range(repeats):
        start = time.perf_counter()
        output = fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples), output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actors", type=int, default=100)
    parser.add_argument("--activities", type=int, default=100)
    parser.add_argument("--notifications", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="medium",
                        help="fraction of relation-eligible actors referenced in payloads")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    if min(args.actors, args.activities, args.notifications, args.batch_size, args.repeats) < 1:
        parser.error("all numeric arguments must be >= 1")

    db, build_time, db_bytes, eligible, targets = make_db(
        args.actors, args.activities, args.notifications, args.profile, args.seed
    )
    try:
        single_time, single_rows = timed(lambda: one_pass(db), args.repeats)
        paged_time, paged_rows = timed(lambda: paged(db, args.batch_size), args.repeats)
        print(f"Fixture: actors={args.actors}, activities={args.activities}, "
              f"notifications={args.notifications}, profile={args.profile} "
              f"({PROFILES[args.profile]:.0%}), batch_size={args.batch_size}, seed={args.seed}")
        print(f"Build:   {build_time:.3f}s, SQLite pages: {db_bytes / (1024 * 1024):.1f} MiB")
        print(f"Payload target actors: {targets}/{eligible} relation-eligible remote actors")
        print(f"Single pass: median={single_time:.6f}s, candidates={len(single_rows)}")
        print(f"Paged scan:  median={paged_time:.6f}s, candidates={len(paged_rows)}")
        print(f"Same candidate IDs: {sorted(single_rows) == sorted(paged_rows)}")
        if sorted(single_rows) != sorted(paged_rows):
            raise SystemExit("ERROR: query strategies returned different candidate sets")
        print("Note: synthetic in-memory fixture; not a production performance estimate.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
