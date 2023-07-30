package providers

import (
	"github.com/malt3/abstractfs-core/provider"
	"github.com/malt3/abstractfs/cas/memory"
)

var CAS = map[string]provider.Provider{
	"memory": &memory.Provider{},
}
