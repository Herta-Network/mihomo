package route

import (
	"github.com/metacubex/chi"
	"slices"
	"sync"
)

type externalRouter func(r chi.Router)

var externalRouters = make([]externalRouter, 0)
var externalRoutersMutex sync.RWMutex

func Register(route ...externalRouter) {
	externalRoutersMutex.Lock()
	defer externalRoutersMutex.Unlock()
	externalRouters = append(externalRouters, route...)
}

func addExternalRouters(r chi.Router) {
	externalRoutersMutex.RLock()
	routers := slices.Clone(externalRouters)
	externalRoutersMutex.RUnlock()
	for _, caller := range routers {
		caller(r)
	}
}
