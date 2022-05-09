package golden

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
)

func init() {
	rand.Seed(42)
}

type BleveDocument struct {
	Title string
}

type Babbler struct {
	Count     int
	Separator string
	Words     []string
}

func NewBabbler() (b Babbler) {
	b.Count = 10
	b.Separator = " "
	b.Words = []string{
		"instincts",
		"psyche",
		"circlets",
		"motleys",
		"meanness",
		"frankly",
		"countryman",
		"crunchier",
		"banned",
		"charley",
		"tortuously",
		"sherbets",
		"amusements",
		"yearn",
		"barrens",
		"stylistically",
		"furl",
		"chengdu",
		"discharging",
		"crests",
		"lampreys",
		"parapsychology",
		"imbecilities",
		"preshrinks",
		"dolorous",
		"sera",
		"pregnant",
		"olduvai",
		"lightninged",
		"prophylaxis",
		"gnashing",
		"slipping",
		"medleys",
		"ghana",
		"leaner",
		"hilary",
		"steinem",
		"pronghorn",
		"groom",
		"erato",
		"wahhabi",
		"calumniated",
		"cancelation",
		"indeterminate",
		"bedbugs",
		"ailed",
		"inchon",
		"chauffeurs",
		"miaow",
		"lazybones",
		"lapps",
		"detours",
		"pinpointed",
		"guardsman",
		"yarmulke",
		"honied",
		"display",
		"fur",
		"kiddos",
		"overstayed",
		"farts",
		"sweatshirts",
		"issuance",
		"stewardess",
		"smuggled",
		"jimmy",
		"unprincipled",
		"beethoven",
		"irrepressible",
		"reminiscences",
		"sojourns",
		"viciously",
		"coauthored",
		"chid",
		"soothed",
		"patienter",
		"anticlimaxes",
		"commemoration",
		"nanobot",
		"attractiveness",
		"curates",
		"uninformed",
		"scrutinize",
		"delightful",
		"caterers",
		"louts",
		"incubi",
		"shaw",
		"dazzles",
		"comestibles",
		"overdrive",
		"daring",
		"dtp",
		"nonpayments",
		"bandied",
		"carnivore",
		"bird",
		"cinerama",
		"profiles",
		"albuquerque",
	}

	return
}

func (this Babbler) Babble() string {
	pieces := []string{}
	for i := 0; i < this.Count; i++ {
		pieces = append(pieces, this.Words[rand.Int()%len(this.Words)])
	}

	return strings.Join(pieces, this.Separator)
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func TestBleveDeterministic(t *testing.T) {
	t.Parallel()

	var hashSet = make(map[string]bool)

	for iteration := 0; iteration < 10; iteration += 1 {
		var seenDocuments = make(map[string]bool)

		rand.Seed(42)
		babbler := NewBabbler()
		indexMapping := bleve.NewIndexMapping()

		documentMapping := bleve.NewDocumentMapping()
		indexMapping.AddDocumentMapping("Document", documentMapping)

		titleFieldMapping := bleve.NewTextFieldMapping()
		titleFieldMapping.Store = true
		documentMapping.AddFieldMappingsAt("Title", titleFieldMapping)
		index, err := bleve.NewUsing("", indexMapping, bleve.Config.DefaultIndexType, bleve.Config.DefaultMemKVStore, nil)
		if err != nil {
			t.Errorf("Error")
			return
		}
		batch := index.NewBatch()

		for i := 0; i < 50000; i++ {
			docId := fmt.Sprintf("doc-%d", i)
			title := babbler.Babble()
			if !seenDocuments[title] {
				err = batch.Index(docId, BleveDocument{
					Title: title,
				})
				if err != nil {
					t.Errorf("Error")
					return
				}
			} else {
				t.Log("Already seen document")
			}
			seenDocuments[title] = true
		}
		err = index.Batch(batch)
		if err != nil {
			t.Errorf("Error")
			return
		}

		query := bleve.NewMatchQuery(babbler.Babble())
		searchRequest := bleve.NewSearchRequestOptions(query, int(100), 0, true)
		// searchRequest.SortBy([]string{"-_score", "Title"})
		searchRequest.SortByCustom([]search.SearchSort{
			&search.SortField{Field: "-_score"},
			&search.SortField{Field: "Title", Type: search.SortFieldAsString},
		})
		searchRequest.Fields = append(searchRequest.Fields, "Title")
		searchResults, err := index.Search(searchRequest)
		if err != nil {
			t.Errorf("Error")
			return
		}

		results := []string{}
		for _, hit := range searchResults.Hits {
			results = append(results, hit.Fields["Title"].(string))
		}

		md5hash := GetMD5Hash(strings.Join(results, "\n"))
		t.Logf("MD5 for iteration %d is = %s\n", iteration, md5hash)

		hashSet[md5hash] = true
	}

	if len(hashSet) > 1 {
		t.Error("Non deterministic results")
	}
}
