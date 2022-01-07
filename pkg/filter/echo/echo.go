package echo

import (
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/common/extension/filter"
	"github.com/apache/dubbo-go-pixiu/pkg/context/http"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
)

const (
	// Kind is the kind of Fallback.
	Kind = constant.HTTPEchoFilter
)

func init() {
	filter.RegisterHttpFilter(&Plugin{})
}

type (
	// Plugin is http filter plugin.
	Plugin struct {
	}

	// FilterFactory is http filter instance
	FilterFactory struct {
		cfg *Config
	}
	Filter struct {
		cfg *Config
	}

	// Config describe the config of FilterFactory
	Config struct {
		Body string `yaml:"body" json:"body" mapstructure:"body"`
	}
)

func (p *Plugin) Kind() string {
	return Kind
}

func (p *Plugin)CreateFilterFactory() (filter.HttpFilterFactory, error) {

	return &FilterFactory{cfg: &Config{}}, nil
}

func (f *FilterFactory)Config() interface{}  {

	return f.cfg
}

func (f *Filter) Decode(hc *http.HttpContext) filter.FilterStatus {
	_ , err := hc.Writer.Write([]byte(f.cfg.Body))
	if err != nil {
		logger.Errorf("[dubbo-go-pixiu] echo handle context write err:%v!", err)
	}
	return filter.Stop
}

func (f *FilterFactory)Apply() error {

	return nil
}

func (f *FilterFactory)PrepareFilterChain(ctx *http.HttpContext, chain filter.FilterChain) error  {
	fi := &Filter{cfg: f.cfg}
	chain.AppendDecodeFilters(fi)
	return nil
}