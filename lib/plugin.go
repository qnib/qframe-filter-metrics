package qframe_filter_metrics

import (
	"fmt"
	"time"
	"strings"
	"strconv"
	"github.com/zpatrick/go-config"
	"github.com/qnib/qframe-types"

)

const (
	version   = "0.2.0"
	pluginTyp = "filter"
	pluginPkg = "metric"
)

type Plugin struct {
	qtypes.Plugin
}

func New(qChan qtypes.QChan, cfg *config.Config, name string) (p Plugin, err error) {
	p = Plugin{
		Plugin: qtypes.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}
	return
}

// Run fetches everything from the Data channel and flushes it to stdout
func (p *Plugin) Run() {
	p.Log("notice", fmt.Sprintf("Start plugin v%s", p.Version))
	dc := p.QChan.Data.Join()
	for {
		select {
		case val := <-dc.Read:
			switch val.(type) {
			case qtypes.Message:
				msg := val.(qtypes.Message)
				if p.StopProcessingMessage(msg, false) {
					continue
				}
				name, nok := msg.KV["name"]
				tval, tok := msg.KV["time"]
				value, vok := msg.KV["value"]
				if nok && tok && vok {
					mval, _ := strconv.ParseFloat(value, 64)
					tint, _ := strconv.Atoi(tval)
					dims := map[string]string{}
					met := qtypes.NewExt(p.Name, name, qtypes.Gauge, mval, dims, time.Unix(int64(tint), 0), true)
					tags, tagok := msg.KV["tags"]
					if tagok {
						for _, item := range strings.Split(tags, ",") {
							dim := strings.Split(item, "=")
							if len(dim) == 2 {
								met.Dimensions[dim[0]] = dim[1]
							}
						}
					}
					p.Log("trace", "send metric")
					p.QChan.Data.Send(met)
				}
			}
		}
	}
}
