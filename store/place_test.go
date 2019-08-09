package store

import (
	"context"
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/api/option"
)

func TestGetPlaceKML(t *testing.T) {
	ctx := context.Background()
	dcid := "geoId/06"
	kml := "<coordinates>1,2,3 4,5,6</coordinates>"

	conn, err := setupBigtable(ctx, map[string]string{
		util.BtPlaceKMLPrefix + dcid: kml,
	})
	if err != nil {
		t.Fatalf("setupBigTable() = %v", err)
	}

	store, err := newTestBtStore(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("newTestBtStore() = %v", err)
	}

	var out = pb.GetPlaceKMLResponse{}
	if err := store.GetPlaceKML(ctx, &pb.GetPlaceKMLRequest{Dcid: dcid}, &out); err != nil {
		t.Fatalf("GetPlaceKML() = %v", err)
	}
	want := &pb.GetPlaceKMLResponse{
		Payload: "<coordinates>1,2,3 4,5,6</coordinates>",
	}
	if got := &out; !reflect.DeepEqual(got, want) {
		t.Errorf("GetPlaceKML() = %v, want %v", got, want)
	}
}
