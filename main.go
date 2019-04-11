package main

import (
	"context"
    "flag"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"reflect"
	"strings"
	_	"github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*findTerm)(nil)).Elem())
}

type findTerm struct {
	SearchTerm string
}

var (
	ctx = context.Background()
	input = flag.String("input", "gs://spikey-data-flow-store/data/spikey_winery_list.csv", "File(s) to read.")
	
	// Set this required option to specify where to write the output.
	output = flag.String("output", "gs://spikey-data-flow-store/output/wineries/california", "Output file (required).")
)

func main() {
	flag.Parse()
	p := beam.NewPipeline()
	s := p.Root()

	searchTerm := "California"

	lines := textio.Read(s, *input)
	found := beam.ParDo(s, &findTerm{SearchTerm: searchTerm}, lines)
	textio.Write(s, *output, found)
	
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

func (ft *findTerm) ProcessElement(line string) string {
	if strings.Contains(line, ft.SearchTerm) {
		return line
	}
	
	return ""
}
