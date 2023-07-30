package casserve

import (
	"context"
	"net"
	"net/http"

	"github.com/malt3/abstractfs-core/api"
	corehttp "github.com/malt3/abstractfs-core/cas/http"
)

type httpServer struct {
	http.Server
	listener net.Listener
}

func newHTTPServer(cas api.CAS, listener net.Listener) runnable {
	return &httpServer{
		Server: http.Server{
			Handler: corehttp.NewHandler(cas),
		},
		listener: listener,
	}
}

func (s *httpServer) Serve(_ context.Context) error {
	return s.Server.Serve(s.listener)
}
