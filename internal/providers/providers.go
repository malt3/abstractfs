package providers

import (
	"github.com/malt3/abstractfs-core/provider"
	"github.com/malt3/abstractfs/fs/dir"
	"github.com/malt3/abstractfs/fs/tar"
)

var All = map[string]provider.Provider{
	"dir": &dir.Provider{},
	"tar": &tar.Provider{},
}
