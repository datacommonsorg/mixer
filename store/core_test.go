package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/api/option"
)

func TestGetPropertyLabels(t *testing.T) {
	ctx := context.Background()

	data := map[string]string{}
	resultMap := map[string]*PropLabelCache{}
	for _, d := range []struct {
		dcid   string
		labels *PropLabelCache
	}{
		{
			"geoId/06",
			&PropLabelCache{
				InLabels:  []string{"containedIn"},
				OutLabels: []string{"containedIn", "name", "longitude"},
			},
		},
		{
			"bio/tiger",
			&PropLabelCache{
				OutLabels: []string{"name", "longitude", "color"},
			},
		},
	} {
		jsonRaw, err := json.Marshal(d.labels)
		if err != nil {
			t.Fatalf("json.Marshal(%v) = %v", d.dcid, err)
		}
		tableValue, err := util.ZipAndEncode(string(jsonRaw))
		if err != nil {
			t.Fatalf("util.ZipAndEncode(%v) = %v", d.dcid, err)
		}
		data[util.BtArcsPrefix+d.dcid] = tableValue

		if d.labels.InLabels == nil {
			d.labels.InLabels = []string{}
		}
		if d.labels.OutLabels == nil {
			d.labels.OutLabels = []string{}
		}
		resultMap[d.dcid] = d.labels
	}

	wantPayloadRaw, err := json.Marshal(resultMap)
	if err != nil {
		t.Fatalf("json.Marshal(%v) = %v", resultMap, err)
	}
	want := &pb.GetPropertyLabelsResponse{
		Payload: string(wantPayloadRaw),
	}

	conn, err := setupBigtable(ctx, data)
	if err != nil {
		t.Fatalf("setupBigTable() = %v", err)
	}

	store, err := newTestBtStore(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("newTestBtStore() = %v", err)
	}

	var out pb.GetPropertyLabelsResponse
	if err := store.GetPropertyLabels(ctx,
		&pb.GetPropertyLabelsRequest{
			Dcids: []string{"geoId/06", "bio/tiger"},
		}, &out); err != nil {
		t.Fatalf("GetPropertyLabels() = %v", err)
	}

	if got := &out; !reflect.DeepEqual(got, want) {
		t.Errorf("GetPropertyLabels() = %v, want %v", got, want)
	}
}
