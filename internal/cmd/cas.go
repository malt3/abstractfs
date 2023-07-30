package cmd

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/malt3/abstractfs/internal/casserve"
	"github.com/spf13/cobra"
)

// NewCASCmd creates a new cas command.
func NewCASCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cas",
		Short: "Serves a CAS",
		Long:  "Serves a content addressable storage (CAS).",
		Args:  cobra.ExactArgs(0),
		RunE:  runCAS,
	}

	cmd.SetOut(os.Stdout)

	cmd.Flags().String("backend-type", "", "Type of the CAS backend.")
	cmd.Flags().StringToString("backend-option", nil, "Optional CAS backend specific options.")
	cmd.Flags().StringSlice("http-listen", nil, "Optional address (tcp or unix domain socket) to listen on for HTTP requests.")
	cmd.Flags().StringSlice("record-listen", nil, "Optional address (tcp or unix domain socket) to listen on for recording requests.")
	cmd.Flags().StringSlice("record-from", nil, "Optional file to read records from. Use \"-\" for stdin.")
	cmd.Flags().Bool("verbose", false, "Enable verbose output")
	must(cmd.MarkFlagRequired("backend-type"))

	return cmd
}

func runCAS(cmd *cobra.Command, args []string) error {
	flags, err := parseCASFlags(cmd)
	if err != nil {
		return err
	}

	backend, closeBackend, err := getCASBackend(flags.BackendType, flags.BackendOpts)
	if err != nil {
		return err
	}
	defer closeBackend()

	server := casserve.New(backend)
	defer server.Stop()

	for _, conf := range flags.HTTPListeners {
		listener, err := net.Listen(conf.Network, conf.Address)
		if err != nil {
			return err
		}
		defer listener.Close()
		server.AddHTTPListener(listener)
	}

	for _, conf := range flags.RecordListen {
		listener, err := net.Listen(conf.Network, conf.Address)
		if err != nil {
			return err
		}
		defer listener.Close()
		server.AddRecorderListener(listener)
	}

	for _, from := range flags.RecordFrom {
		var reader io.Reader
		if from == "-" {
			reader = os.Stdin
		} else {
			f, err := os.Open(from)
			if err != nil {
				return err
			}
			defer f.Close()
			reader = f
		}
		server.AddRecorder(reader)
	}

	return server.Serve(cmd.Context())
}

type casFlags struct {
	BackendType   string
	BackendOpts   map[string]string
	HTTPListeners []listenConfig
	RecordListen  []listenConfig
	RecordFrom    []string
	Verbose       bool
}

func parseCASFlags(cmd *cobra.Command) (casFlags, error) {
	backendType, err := cmd.Flags().GetString("backend-type")
	if err != nil {
		return casFlags{}, err
	}
	backendOptions, err := cmd.Flags().GetStringToString("backend-option")
	if err != nil {
		return casFlags{}, err
	}
	httpListenersURLs, err := cmd.Flags().GetStringSlice("http-listen")
	if err != nil {
		return casFlags{}, err
	}
	httpListeners := make([]listenConfig, 0, len(httpListenersURLs))
	for _, u := range httpListenersURLs {
		l, err := parseListenURL(u)
		if err != nil {
			return casFlags{}, err
		}
		httpListeners = append(httpListeners, l)
	}
	recordListenersURLs, err := cmd.Flags().GetStringSlice("record-listen")
	if err != nil {
		return casFlags{}, err
	}
	recordListeners := make([]listenConfig, 0, len(recordListenersURLs))
	for _, u := range recordListenersURLs {
		l, err := parseListenURL(u)
		if err != nil {
			return casFlags{}, err
		}
		recordListeners = append(recordListeners, l)
	}
	recordFrom, err := cmd.Flags().GetStringSlice("record-from")
	if err != nil {
		return casFlags{}, err
	}

	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		return casFlags{}, err
	}

	return casFlags{
		BackendType:   backendType,
		BackendOpts:   backendOptions,
		HTTPListeners: httpListeners,
		RecordListen:  recordListeners,
		RecordFrom:    recordFrom,
		Verbose:       verbose,
	}, nil
}

func parseListenURL(u string) (listenConfig, error) {
	listenURL, err := url.Parse(u)
	if err != nil {
		return listenConfig{}, err
	}
	var address string
	switch listenURL.Scheme {
	case "tcp", "tcp4", "tcp6":
		address = listenURL.Host
	case "unix":
		if len(listenURL.Host) > 0 {
			address = path.Join(listenURL.Host, listenURL.Path)
		} else {
			address = listenURL.Path
		}
	default:
		return listenConfig{}, fmt.Errorf("invalid scheme: %s", listenURL.Scheme)
	}
	query := listenURL.Query()
	var opts map[string]string
	if len(query) > 0 {
		opts = make(map[string]string, len(query))
	}
	for k, v := range query {
		vRaw := strings.Join(v, ",")
		opts[k] = vRaw
	}
	return listenConfig{
		Network: listenURL.Scheme,
		Address: address,
		Options: opts,
	}, nil
}

type listenConfig struct {
	Network, Address string
	Options          map[string]string
}
