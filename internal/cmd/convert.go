package cmd

import (
	"fmt"
	"os"

	"github.com/malt3/abstractfs-core/api"
	coretree "github.com/malt3/abstractfs-core/tree"
	"github.com/spf13/cobra"
)

// NewConvertCmd creates a new convert command.
func NewConvertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "convert",
		Short: "Converts a file system to a different format",
		Long:  "Converts a file system to a different format.",
		Args:  cobra.ExactArgs(0),
		RunE:  runConvert,
	}

	cmd.SetOut(os.Stdout)

	cmd.Flags().String("source", "", "Path or reference to the source.")
	cmd.Flags().String("source-type", "", "Type of the source.")
	cmd.Flags().String("sink", "", "Path or reference to the sink.")
	cmd.Flags().String("sink-type", "", "Type of the sink.")
	cmd.Flags().Bool("verbose", false, "Enable verbose output")
	must(cmd.MarkFlagRequired("source"))
	must(cmd.MarkFlagRequired("source-type"))
	must(cmd.MarkFlagRequired("sink"))
	must(cmd.MarkFlagRequired("sink-type"))

	return cmd
}

func runConvert(cmd *cobra.Command, args []string) error {
	flags, err := parseConvertFlags(cmd)
	if err != nil {
		return err
	}

	source, closeSource, err := getSource(flags.Source, flags.SourceType)
	if err != nil {
		return err
	}
	defer closeSource()

	casReader, ok := source.(api.CASReader)
	if !ok {
		return fmt.Errorf("source does not support reading as CAS")
	}

	tree, err := coretree.FromSource(source)
	if err != nil {
		return err
	}

	treeFS := &coretree.TreeFS{Tree: tree, CASReader: casReader}

	sink, closeSink, err := getSink(flags.Sink, flags.SinkType)
	if err != nil {
		return err
	}
	defer closeSink()
	return sink.Consume(treeFS)
}

type convertFlags struct {
	Source     string
	SourceType string
	Sink       string
	SinkType   string
	Verbose    bool
}

func parseConvertFlags(cmd *cobra.Command) (convertFlags, error) {
	source, err := cmd.Flags().GetString("source")
	if err != nil {
		return convertFlags{}, err
	}
	sourceType, err := cmd.Flags().GetString("source-type")
	if err != nil {
		return convertFlags{}, err
	}
	sink, err := cmd.Flags().GetString("sink")
	if err != nil {
		return convertFlags{}, err
	}
	sinkType, err := cmd.Flags().GetString("sink-type")
	if err != nil {
		return convertFlags{}, err
	}
	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		return convertFlags{}, err
	}

	return convertFlags{
		Source:     source,
		SourceType: sourceType,
		Sink:       sink,
		SinkType:   sinkType,
		Verbose:    verbose,
	}, nil
}
