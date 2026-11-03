#!/usr/bin/env python3
"""Isolated SQLite benchmark for remote ActivityPub actor cleanup query shapes.

This script creates an in-memory synthetic database. It never opens a
PocketBase database and never connects to Meilisearch or any network service.

It compares:
  1. one set-based scan over all actors;
  2. keyset-paged scans that evaluate at most --batch-size actors per query.

The fixture is deliberately small by default (100 actors, 100 activities,
100 notifications) so it completes quickly. Results are useful for comparing
query shape and correctness, not for predicting production wall-clock time.
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import statistics
import time


def make_db(actor_count: int, activity_count: int, notification_count: int) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.executescript("""
        PRAGMA temp_store = MEMORY;
        CREATE TABLE activitypub_actors (
            id TEXT PRIMARY KEY,
            iri TEXT NOT NULL,
            preferred_username TEXT NOT NULL,
            domain TEXT NOT NULL,
            is_local INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE activitypub_activities (
            id INTEGER PRIMARY KEY,
            actor TEXT,
            object TEXT,
            cc TEXT,
            "to" TEXT,
            relation TEXT
        );
        CREATE TABLE notifications (
            id INTEGER PRIMARY KEY,
            metadata TEXT
        );
        CREATE TABLE relation_refs (actor_id TEXT NOT NULL);
        CREATE INDEX idx_relation_refs_actor_id ON relation_refs(actor_id);
    """)

    actors = []
    for i in range(actor_count):
        actor_id = f"actor{i:06d}"
        actors.append((
            actor_id,
            f"https://remote{i}.example/users/user{i}",
            f"user{i}",
            f"remote{i}.example",
            0,
        ))
    db.executemany(
        "INSERT INTO activitypub_actors(id, iri, preferred_username, domain, is_local) VALUES (?, ?, ?, ?, ?)",
        actors,
    )

    # Half of the actors are already referenced by a PocketBase relation.
    db.executemany(
        "INSERT INTO relation_refs(actor_id) VALUES (?)",
        [(actors[i][0],) for i in range(0, actor_count, 2)],
    )

    rng = random.Random(20261103)
    activities = []
    for i in range(activity_count):
        # A modest number of payloads mention an actor; most do not.
        if actors and i % 10 == 0:
            actor = actors[rng.randrange(len(actors))]
            payload = actor[1]
        else:
            payload = f"https://unrelated.example/objects/{i}"
        activities.append((
            i,
            "https://local.example/users/owner",
            payload,
            "",
            "",
            "",
        ))
    db.executemany(
        'INSERT INTO activitypub_activities(id, actor, object, cc, "to", relation) VALUES (?, ?, ?, ?, ?, ?)',
        activities,
    )

    notifications = []
    for i in range(notification_count):
        if actors and i % 20 == 0:
            actor = actors[rng.randrange(len(actors))]
            metadata = f'{{"actor":"{actor[1]}","handle":"@{actor[2]}@{actor[3]}"}}'
        else:
            metadata = f'{{"message":"notification {i}"}}'
        notifications.append((i, metadata))
    db.executemany(
        "INSERT INTO notifications(id, metadata) VALUES (?, ?)",
        notifications,
    )
    return db


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


def one_pass(db: sqlite3.Connection) -> list[str]:
    query = """
    WITH candidate_actors AS MATERIALIZED (
        SELECT a.id, a.iri, a.preferred_username, a.domain
        FROM activitypub_actors a
        WHERE a.is_local = 0 AND a.iri <> ''
          AND NOT EXISTS (
              SELECT 1 FROM relation_refs r WHERE r.actor_id = a.id
          )
    ),
    """ + PAYLOAD_CTES + """
    SELECT ca.id
    FROM candidate_actors ca
    WHERE NOT EXISTS (SELECT 1 FROM payload_refs p WHERE p.actor_id = ca.id)
    ORDER BY ca.id
    """
    return [row[0] for row in db.execute(query)]


def paged(db: sqlite3.Connection, batch_size: int) -> list[str]:
    results: list[str] = []
    after = ""
    while True:
        batch = db.execute("""
            SELECT id, iri, preferred_username, domain
            FROM activitypub_actors
            WHERE is_local = 0 AND iri <> '' AND id > ?
            ORDER BY id
            LIMIT ?
        """, (after, batch_size)).fetchall()
        if not batch:
            break
        after = batch[-1][0]

        db.execute("DROP TABLE IF EXISTS temp.actor_batch")
        db.execute("""
            CREATE TEMP TABLE actor_batch (
                id TEXT PRIMARY KEY,
                iri TEXT,
                preferred_username TEXT,
                domain TEXT
            )
        """)
        db.executemany(
            "INSERT INTO actor_batch(id, iri, preferred_username, domain) VALUES (?, ?, ?, ?)",
            batch,
        )
        query = """
        WITH candidate_actors AS MATERIALIZED (
            SELECT a.id, a.iri, a.preferred_username, a.domain
            FROM actor_batch a
            WHERE NOT EXISTS (
                SELECT 1 FROM relation_refs r WHERE r.actor_id = a.id
            )
        ),
        """ + PAYLOAD_CTES + """
        SELECT ca.id
        FROM candidate_actors ca
        WHERE NOT EXISTS (SELECT 1 FROM payload_refs p WHERE p.actor_id = ca.id)
        ORDER BY ca.id
        """
        results.extend(row[0] for row in db.execute(query))
    return results


def timed(fn, repeats: int) -> tuple[float, list[str]]:
    samples = []
    output: list[str] = []
    for _ in range(repeats):
        started = time.perf_counter()
        output = fn()
        samples.append(time.perf_counter() - started)
    return statistics.median(samples), output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actors", type=int, default=100)
    parser.add_argument("--activities", type=int, default=100)
    parser.add_argument("--notifications", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()

    if min(args.actors, args.activities, args.notifications, args.batch_size, args.repeats) < 1:
        parser.error("all numeric arguments must be >= 1")

    db = make_db(args.actors, args.activities, args.notifications)
    try:
        single_time, single_rows = timed(lambda: one_pass(db), args.repeats)
        paged_time, paged_rows = timed(lambda: paged(db, args.batch_size), args.repeats)

        print(f"Fixture: actors={args.actors}, activities={args.activities}, "
              f"notifications={args.notifications}, batch_size={args.batch_size}")
        print(f"Single pass: median={single_time:.6f}s, candidates={len(single_rows)}")
        print(f"Paged scan:  median={paged_time:.6f}s, candidates={len(paged_rows)}")
        print(f"Same candidate IDs: {single_rows == paged_rows}")
        if single_rows != paged_rows:
            raise SystemExit("ERROR: query strategies returned different candidate sets")
        print("Note: in-memory synthetic fixture only; not a production performance estimate.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
