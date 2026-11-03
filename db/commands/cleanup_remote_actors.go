package commands

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pocketbase/pocketbase"
	"github.com/spf13/cobra"

	"pocketbase/util"
)

// CleanupRemoteActors returns a conservative, resumable remote actor cache
// cleanup command. It is a dry-run unless --execute is explicitly supplied.
func CleanupRemoteActors(app *pocketbase.PocketBase) *cobra.Command {
	var (
		execute        bool
		afterID        string
		maxActors      int
		checkpointFile string
	)

	cmd := &cobra.Command{
		Use:   "cleanup-remote-actors",
		Short: "Find and optionally remove unreferenced remote ActivityPub actors",
		Long: "Streams remote ActivityPub actors with no known references in stable ID order. " +
			"By default it only reports candidates. Use --execute to delete candidates locally; " +
			"references are checked again inside the deletion transaction. " +
			"Resume an interrupted scan with --after-id or --checkpoint-file.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if maxActors < 0 {
				return fmt.Errorf("max-actors cannot be negative")
			}

			processed, deleted, failed := 0, 0, 0
			cursor := strings.TrimSpace(afterID)
			if cursor == "" && checkpointFile != "" {
				data, err := os.ReadFile(checkpointFile)
				if err == nil {
					cursor = strings.TrimSpace(string(data))
				} else if !os.IsNotExist(err) {
					return fmt.Errorf("read checkpoint %s: %w", checkpointFile, err)
				}
			}

			cmd.Printf("Remote actor cleanup: execute=%t after-id=%q\n", execute, cursor)
			stopAfterLimit := errors.New("candidate limit reached")
			stopped := false

			err := util.StreamUnreferencedRemoteActors(app, cursor, func(candidate util.RemoteActorCandidate) error {
				if maxActors > 0 && processed >= maxActors {
					stopped = true
					return stopAfterLimit
				}

				cursor = candidate.ID
				processed++
				cmd.Printf("CANDIDATE\t%s\t%s\t@%s@%s\n",
					candidate.ID, candidate.IRI, candidate.PreferredUsername, candidate.Domain)

				if execute {
					actor, err := app.FindRecordById("activitypub_actors", candidate.ID)
					if err != nil {
						return fmt.Errorf("reload candidate %s: %w", candidate.ID, err)
					}
					if err := util.DeleteUnreferencedRemoteActor(app, actor); err != nil {
						failed++
						cmd.Printf("FAILED\t%s\t%v\n", candidate.ID, err)
						return fmt.Errorf("stop after failed deletion of %s: %w", candidate.ID, err)
					}
					deleted++
					cmd.Printf("DELETED\t%s\n", candidate.ID)
				}

				if checkpointFile != "" {
					if err := os.WriteFile(checkpointFile, []byte(cursor+"\n"), 0600); err != nil {
						return fmt.Errorf("write checkpoint %s: %w", checkpointFile, err)
					}
				}
				return nil
			})
			if err != nil && !errors.Is(err, stopAfterLimit) {
				return err
			}

			mode := "dry-run"
			if execute {
				mode = "execute"
			}
			cmd.Printf("Summary (%s): candidates=%d deleted=%d failed=%d last-id=%s stopped=%t\n",
				mode, processed, deleted, failed, cursor, stopped)
			return nil
		},
	}

	cmd.Flags().BoolVar(&execute, "execute", false, "Actually delete candidates (default: report only)")
	cmd.Flags().StringVar(&afterID, "after-id", "", "Resume after this actor record ID")
	cmd.Flags().IntVar(&maxActors, "max-actors", 0, "Stop after processing this many candidates (0 = no limit)")
	cmd.Flags().StringVar(&checkpointFile, "checkpoint-file", "", "Read the starting ID if present and write the last processed ID here")
	return cmd
}
