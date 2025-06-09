package rdb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ZaparooProject/zaparoo-titles-database/pkg/settings"
)

type RdbJsonROM struct {
	ID           int    `json:"id"`
	Serial       string `json:"serial"`
	MD5          string `json:"md5"`
	SHA1         string `json:"sha1"`
	CRC          string `json:"crc"`
	Size         int    `json:"size"`
	RomName      string `json:"rom_name"`
	Region       string `json:"region"`
	Description  string `json:"description"`
	Name         string `json:"name"`
	Publisher    string `json:"publisher"`
	Developer    string `json:"developer"`
	ReleaseYear  int    `json:"releaseyear"`
	ReleaseMonth int    `json:"releasemonth"`
	Users        int    `json:"users"`
	Genre        string `json:"genre"`
	Franchise    string `json:"franchise"`
	RDBName      string `json:"rbd_name"`
	UniqueKey    string `json:"unique_key"`
	UniqueType   string `json:"unique_type"`
}

func MakeNDJSON(rdbFileName string) {
	toolPath := "./libretrodb_tool"
	rdbDir := "./assets/rdb"
	ndjsonDir := "./assets/ndjson"
	rdbPath := filepath.Join(rdbDir, rdbFileName)
	ndjsonPath := filepath.Join(ndjsonDir, rdbFileName+".ndjson")
	// open the out file for writing
	outfile, err := os.Create(ndjsonPath)
	if err != nil {
		fmt.Println("Cannot create NDJSON", ndjsonPath, rdbFileName)
		return
	}
	defer outfile.Close()

	cmd := exec.Command(toolPath, rdbPath, "list")
	cmd.Stdout = outfile

	err = cmd.Run()
	if err != nil {
		fmt.Println("Error running libretro_tool", rdbFileName)
		fmt.Println(err)
		return
	}
}

func LoadNDJSON(rdbName string) ([]RdbJsonROM, error) {
	ndjsonPath := filepath.Join(settings.NdjsonDir, rdbName+".ndjson")
	defaultRoms := make([]RdbJsonROM, 0)
	fmt.Printf("Opening %s\n", ndjsonPath)
	rdbFile, err := os.Open(ndjsonPath)
	if err != nil {
		fmt.Printf("Error openingT %s\n", ndjsonPath)
		return defaultRoms, err
	}
	fmt.Printf("Trying ReadAll from Local %s\n", ndjsonPath)
	rdbBytes, err := io.ReadAll(rdbFile)
	if err != nil && err != io.EOF {
		fmt.Printf("Unable parse local bytes %s, skipping core\n", ndjsonPath)
		return defaultRoms, err
	}
	return ParseNDJSON(rdbBytes, rdbName)
}

func ParseNDJSON(jsonStream []byte, rdbName string) ([]RdbJsonROM, error) {
	// escape / char
	//stringContent := strings.ReplaceAll(string(jsonStream[:]), "\\", "/")

	dec := json.NewDecoder(strings.NewReader(string(jsonStream[:])))

	roms := make([]RdbJsonROM, 0)
	for {
		var jsonRom RdbJsonROM
		if err := dec.Decode(&jsonRom); err == io.EOF {
			break
		} else if err != nil {
			return roms, err
		}
		jsonRom.RDBName = rdbName
		//fmt.Printf("%#v\n", jsonRom)
		roms = append(roms, jsonRom)
	}
	return roms, nil
}

func MarshalRomJson(rom RdbJsonROM) (string, error) {
	b, err := json.Marshal(rom)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func UnmarshalRomJson(romJson string) (RdbJsonROM, error) {
	var rom RdbJsonROM
	err := json.Unmarshal([]byte(romJson), &rom)
	return rom, err
}
