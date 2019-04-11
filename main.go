package main

import (
	"context"
	"flag"
	"log"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

type findTerm struct {
	SearchTerm string
}

var (
	ctx context.Context

	input   = flag.String("input", "gs://spikey-data-flow-store/data/spikey_winery_list.csv", "File(s) to read.")
	output  = flag.String("output", "gs://spikey-data-flow-store/output/wineries/california", "Output file (required).")
	lineLen = beam.NewDistribution("extract", "lenLenDistro")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*findTerm)(nil)).Elem())
	ctx = context.Background()
}

func main() {
	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup. On
	// distributed runners, it is used to intercept control.
	beam.Init()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Fatal("No output provided")
	}

	// Concepts #3 and #4: The pipeline uses the named transform and DoFn.
	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	found := beam.ParDo(s, &findTerm{SearchTerm: "California"}, lines)
	textio.Write(s, *output, found)

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func (ft *findTerm) ProcessElement(line string, emit func(string)) {
	if strings.Contains(line, ft.SearchTerm) {
		emit(line)
	}
}
