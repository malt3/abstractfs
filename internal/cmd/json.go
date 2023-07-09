package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/malt3/abstractfs-core/api"
	coretree "github.com/malt3/abstractfs-core/tree"
	"github.com/malt3/abstractfs/fs/dir"
	"github.com/malt3/abstractfs/fs/tar"
	"github.com/spf13/cobra"
)

// NewJSONCmd creates a new json command.
func NewJSONCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "json",
		Short: "Converts a source file system to json",
		Long:  "Converts a source file system to json.",
		Args:  cobra.ExactArgs(0),
		RunE:  runJSON,
	}

	cmd.SetOut(os.Stdout)

	cmd.Flags().String("source", "", "Path or reference to the source.")
	cmd.Flags().String("source-type", "", "Type of the source.")
	cmd.Flags().String("out", "", "Optional path to write the upload result to. If not set, the result is written to stdout.")
	cmd.Flags().Bool("verbose", false, "Enable verbose output")
	must(cmd.MarkFlagRequired("source"))
	must(cmd.MarkFlagRequired("source-type"))

	return cmd
}

func runJSON(cmd *cobra.Command, args []string) error {
	flags, err := parseJSONFlags(cmd)
	if err != nil {
		return err
	}

	source, closeSource, err := getSource(flags.Source, flags.SourceType)
	if err != nil {
		return err
	}
	defer closeSource()

	tree, err := coretree.FromSource(source)
	if err != nil {
		return err
	}

	flat := coretree.Flatten(tree)

	out := cmd.OutOrStdout()
	if len(flags.Out) > 0 {
		outF, err := os.Create(flags.Out)
		if err != nil {
			return fmt.Errorf("json: opening output file: %w", err)
		}
		defer outF.Close()
		out = outF
	}
	return json.NewEncoder(out).Encode(flat.Files)
}

func getSource(source, sourceType string) (api.Source, func(), error) {
	switch sourceType {
	case "dir":
		source, closer := dir.New(source).Source(dir.Options{})
		return source, closer, nil
	case "tar":
		sourceFile, err := os.Open(source)
		if err != nil {
			return nil, nil, fmt.Errorf("json: opening tar file: %w", err)
		}
		source, closer := tar.NewReadFS(sourceFile).Source(tar.Options{})
		return source,
			func() {
				closer()
				sourceFile.Close()
			},
			nil
	}
	return nil, nil, errors.New("unsupported source")
}

type jsonFlags struct {
	Source     string
	SourceType string
	Out        string
	Verbose    bool
}

func parseJSONFlags(cmd *cobra.Command) (jsonFlags, error) {
	source, err := cmd.Flags().GetString("source")
	if err != nil {
		return jsonFlags{}, err
	}
	sourceType, err := cmd.Flags().GetString("source-type")
	if err != nil {
		return jsonFlags{}, err
	}
	out, err := cmd.Flags().GetString("out")
	if err != nil {
		return jsonFlags{}, err
	}

	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		return jsonFlags{}, err
	}

	return jsonFlags{
		Source:     source,
		SourceType: sourceType,
		Out:        out,
		Verbose:    verbose,
	}, nil
}
