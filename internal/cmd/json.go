package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/malt3/abstractfs-core/api"
	coretree "github.com/malt3/abstractfs-core/tree"
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
	cmd.Flags().String("out", "", "Optional path to write the JSON to. If not set, the result is written to stdout.")
	cmd.Flags().StringToString("source-option", nil, "Optional provider specific options.")
	cmd.Flags().StringSlice("record-to", nil, "Optional output url to record CAS contents to.")
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

	source, closeSource, err := getSource(flags.Source, flags.SourceType, flags.SourceOpts)
	if err != nil {
		return err
	}
	defer closeSource()

	tree, err := coretree.FromSource(source)
	if err != nil {
		return err
	}

	if err := record(source, tree, flags); err != nil {
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

func record(source api.Source, tree api.Tree, flags jsonFlags) error {
	if len(flags.RecordTo) == 0 {
		return nil
	}

	casReader, ok := source.(api.CASReader)
	if !ok {
		return fmt.Errorf("source does not support reading as CAS")
	}
	treeFS := &coretree.TreeFS{Tree: tree, CASReader: casReader}

	var writers []io.Writer
	for _, conf := range flags.RecordTo {
		switch conf.Protocol {
		case "tcp", "tcp4", "tcp6", "unix":
			conn, err := net.Dial(conf.Protocol, conf.Location)
			if err != nil {
				return err
			}
			defer conn.Close()
			writers = append(writers, conn)
		case "file":
			// TODO: support append via option
			f, err := os.OpenFile(conf.Location, os.O_WRONLY|os.O_CREATE, os.ModePerm)
			if err != nil {
				return err
			}
			defer f.Close()
			writers = append(writers, f)
		default:
			return fmt.Errorf("invalid protocol: %s", conf.Protocol)
		}
	}

	return treeFS.Record(io.MultiWriter(writers...))
}

type jsonFlags struct {
	Source     string
	SourceType string
	SourceOpts map[string]string
	RecordTo   []recordToConfig
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
	sourceOptions, err := cmd.Flags().GetStringToString("source-option")
	if err != nil {
		return jsonFlags{}, err
	}
	recordTo, err := cmd.Flags().GetStringSlice("record-to")
	if err != nil {
		return jsonFlags{}, err
	}
	recordToConfigs := make([]recordToConfig, 0, len(recordTo))
	for _, u := range recordTo {
		recordToConfig, err := parseRecordURL(u)
		if err != nil {
			return jsonFlags{}, err
		}
		recordToConfigs = append(recordToConfigs, recordToConfig)
	}

	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		return jsonFlags{}, err
	}

	return jsonFlags{
		Source:     source,
		SourceType: sourceType,
		SourceOpts: sourceOptions,
		RecordTo:   recordToConfigs,
		Out:        out,
		Verbose:    verbose,
	}, nil
}

func parseRecordURL(u string) (recordToConfig, error) {
	recordURL, err := url.Parse(u)
	if err != nil {
		return recordToConfig{}, err
	}
	var location string
	switch recordURL.Scheme {
	case "tcp", "tcp4", "tcp6":
		location = recordURL.Host
	case "unix", "file":
		if len(recordURL.Host) > 0 {
			location = path.Join(recordURL.Host, recordURL.Path)
		} else {
			location = recordURL.Path
		}

	default:
		return recordToConfig{}, fmt.Errorf("invalid scheme: %s", recordURL.Scheme)
	}
	query := recordURL.Query()
	var opts map[string]string
	if len(query) > 0 {
		opts = make(map[string]string, len(query))
	}
	for k, v := range query {
		vRaw := strings.Join(v, ",")
		opts[k] = vRaw
	}
	return recordToConfig{
		Protocol: recordURL.Scheme,
		Location: location,
		Options:  opts,
	}, nil
}

type recordToConfig struct {
	Protocol, Location string
	Options            map[string]string
}
