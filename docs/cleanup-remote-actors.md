# Cleanup of unreferenced remote ActivityPub actors

The `cleanup-remote-actors` PocketBase command scans remote ActivityPub actor
records and reports those that appear to have no remaining local references.
It is intended to remove stale remote-actor cache entries, not user content.

## Safety model

- **Dry-run is the default.** The command does not delete anything unless
  `--execute` is explicitly supplied.
- Candidates are streamed in stable record-ID order. The scan can be limited
  with `--max-actors` and resumed with `--after-id` or `--checkpoint-file`.
- Before deletion, the actor is reloaded and its reference graph is checked
  again inside the PocketBase deletion transaction. If a reference is found,
  deletion is refused.
- The reference check covers relation fields pointing to
  `activitypub_actors`, ActivityPub activity addressing/object fields, and
  notification metadata. Remote-copy content authored by the actor is handled
  by the existing remote-actor deletion cascade.
- Records whose IRI represents a local content identity are protected; the
  deletion is aborted rather than removing locally addressable content.
- Removing a remote actor is a local cache/bookkeeping operation. It does not
  send an ActivityPub Delete to other servers.

## Usage

Run the command using the PocketBase executable and the same configuration
and data directory as the Wanderer instance you intend to maintain.

First, inspect candidates without deleting:

```sh
./pocketbase cleanup-remote-actors --dir=/path/to/pb_data
```

Limit a dry-run to a small number of candidates:

```sh
./pocketbase cleanup-remote-actors --dir=/path/to/pb_data --max-actors=100
```

After reviewing the output and taking a verified backup, explicitly enable
deletion:

```sh
./pocketbase cleanup-remote-actors --dir=/path/to/pb_data --execute
```

Available flags:

| Flag | Meaning |
| --- | --- |
| `--execute` | Perform local deletion. Omit for dry-run. |
| `--max-actors N` | Stop after processing at most N candidates; 0 (default) means no limit. |
| `--after-id ID` | Continue scanning after this actor record ID. |
| `--checkpoint-file PATH` | Read the initial cursor from this file when present and update it after each successfully processed candidate. |

If both `--after-id` and `--checkpoint-file` are supplied, a non-empty
`--after-id` takes precedence. The checkpoint is written with restrictive
permissions (0600). A failed deletion stops the command; the failed candidate
is not checkpointed.

## Search and deletion behavior

The scan identifies possible candidates from the known reference graph. Each
candidate is then checked again transactionally before deletion, so a newly
created reference prevents removal. A dry-run is an inventory at scan time,
not a reservation: references and candidates can change before a later
execution.

The command reports each candidate and ends with a summary containing the
number processed, deleted, failed, the last processed ID, and whether the
requested candidate limit stopped the scan.

## Meilisearch consistency

The existing actor-delete hook removes the corresponding document from
Meilisearch on a best-effort basis. A Meilisearch error is logged but does not
roll back the PocketBase deletion. Therefore this command does **not** provide
transactional consistency between PocketBase and Meilisearch; a failed
Meilisearch task can leave a stale search document that requires separate
reconciliation.

## Operational precautions

Use a verified backup and test the command against a separate copy of the
PocketBase data directory before considering it on a live instance. Do not
point a test run at production data or a production Meilisearch instance.
