package ztdb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ZaparooProject/zaparoo-titles-database/pkg/settings"
)

type System struct {
	ID              int    `json:"id"`
	Name            string `json:"name"`
	ZaparooSystemID string `json:"zaparoo_id"`
	Description     string `json:"description"`
}

type Region struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Language struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Publisher struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Developer struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Genre struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Franchise struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type FileExtension struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type UniqueType struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Title struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type TitleVariant struct {
	ID           int    `json:"id"`
	TitleID      int    `json:"title_id"`
	SystemID     int    `json:"system_id"`
	Filename     string `json:"filename"`
	ReleaseYear  int    `json:"releaseyear"`
	ReleaseMonth int    `json:"releasemonth"`
	Users        int    `json:"users"`
	RegionID     int    `json:"region_id"`
	PublisherID  int    `json:"publisher_id"`
	DeveloperID  int    `json:"developer_id"`
	GenreID      int    `json:"genre_id"`
	FranchiseID  int    `json:"franchise_id"`
	ExtensionID  int    `json:"extension_id"`
	UniqueTypeID int    `json:"unique_type_id"`
	Serial       string `json:"serial"`
	MD5          string `json:"md5"`
	SHA1         string `json:"sha1"`
	CRC          string `json:"crc"`
	Size         int    `json:"size"`
	Name         string `json:"name"`
	Description  string `json:"description"`
}

type GenericDBMeta struct {
	ID          int
	Name        string
	Description string
}

func MarshalMeta(meta any) (string, error) {
	b, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func UnmarshalMeta(meta *any, jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), meta)
}

func LoadNDJSON[T GenericDBMeta | TitleVariant | System](metaType string, metas []T) ([]T, error) {
	ndjsonPath := filepath.Join(settings.DBJsonDir, fmt.Sprintf("_%v.ndjson", metaType))
	fmt.Printf("Opening %s\n", ndjsonPath)
	ndjsonFile, err := os.Open(ndjsonPath)
	if err != nil {
		fmt.Printf("Error openingT %s\n", ndjsonPath)
		return metas, err
	}
	fmt.Printf("Trying ReadAll from Local %s\n", ndjsonPath)
	jsonStream, err := io.ReadAll(ndjsonFile)
	if err != nil && err != io.EOF {
		fmt.Printf("Unable parse local bytes %s, skipping ndjson\n", ndjsonPath)
		return metas, err
	}
	//stringContent := strings.ReplaceAll(string(jsonStream[:]), "\\", "/")
	dec := json.NewDecoder(strings.NewReader(string(jsonStream[:])))
	for {
		var meta T
		if err := dec.Decode(&meta); err == io.EOF {
			break
		} else if err != nil {
			return metas, err
		}
		//fmt.Printf("%#v\n", jsonRom)
		metas = append(metas, meta)
	}
	return metas, nil
}
