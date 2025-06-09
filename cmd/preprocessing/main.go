package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/ZaparooProject/zaparoo-titles-database/pkg/rdb"
	"github.com/ZaparooProject/zaparoo-titles-database/pkg/settings"
	"github.com/ZaparooProject/zaparoo-titles-database/pkg/sqlite"
	"github.com/ZaparooProject/zaparoo-titles-database/pkg/ztdb"
)

/*
Kept for posterity this command set was only used for the initial creating of relational NDJSON files.
*/

const (
	CMDfetchrdbs            string = "fetchrdbs"
	CMDmakendjson           string = "makendjson"
	CMDindexunique          string = "indexunique"
	CMDmakeztdbjsonmeta     string = "makeztdbjsonmeta"
	CMDmakeztdbjsonvariants string = "makeztdbjsonvariants"
)

func main() {
	cmdPtr := flag.String("cmd", "", "[fetchrdbs, makendjson, indexunique, makeztdbjsonmeta, makeztdbjson]")
	flag.Parse()

	switch *cmdPtr {
	case CMDfetchrdbs:
		fetchrdbs()
	case CMDmakendjson:
		makendjson()
	case CMDindexunique:
		indexunique()
	case CMDmakeztdbjsonmeta:
		makeztdbjsonmeta()
	case CMDmakeztdbjsonvariants:
		makeztdbjsonvariants()
	default:
		fmt.Println("no cmd to run")
	}
}

func fetchrdbs() {
	for _, name := range rdb.RBDNames {
		rdb.FetchRDB(name)
	}
}

func makendjson() {
	for _, name := range rdb.RBDNames {
		rdb.MakeNDJSON(name)
	}
}

func indexunique() {
	db, err := sqlite.OpenVariantIndexDB()
	if err != nil {
		fmt.Println("unable open memory DB")
		return
	}
	collisions := make([]rdb.RdbJsonROM, 0)
	for _, rdbName := range rdb.RBDNames {
		rdbRoms, err := rdb.LoadNDJSON(rdbName)
		if err != nil {
			fmt.Println("unable to parse ndjson", rdbName)
		}

		for _, rom := range rdbRoms {
			if rom.RomName == "" {
				//fmt.Println("No ROM name, skipping")
				continue
			}
			romJson, err := rdb.MarshalRomJson(rom)
			if err != nil {
				continue
			}

			err = sqlite.IndexUnique(db, rom.SHA1, "SHA1", rom.RomName, romJson)
			if err == nil {
				continue
			}
			err = sqlite.IndexUnique(db, rom.MD5, "MD5", rom.RomName, romJson)
			if err == nil {
				continue
			}
			err = sqlite.IndexUnique(db, rom.CRC, "CRC32", rom.RomName, romJson)
			if err == nil {
				continue
			}
			err = sqlite.IndexUnique(db, rom.Serial, "SERIAL", rom.RomName, romJson)
			if err == nil {
				continue
			}
			err = sqlite.IndexUnique(db, rom.RomName, "ROM", rom.RomName, romJson)
			if err == nil {
				continue
			}
			err = sqlite.IndexUnique(db, fmt.Sprintf("%v:%v", rom.RomName, rdbName), "ROMSYSTEM", rom.RomName, romJson)
			if err == nil {
				continue
			}
			collisions = append(collisions, rom)
		}
	}
	sqlite.ReindexByFilename(db)
	db.Exec(`vacuum into ?`, settings.DBSqliteUniquePath)
	for _, rom := range collisions {
		fmt.Println("UNIQUE COLLISION", fmt.Sprintf("%+v", rom))
	}
	fmt.Println(len(collisions), "Collision Count")
}

func makeztdbjsonmeta() {
	// Allocate all the structs via maps for simplicity
	db, err := sqlite.OpenUniqueDB()
	if err != nil {
		fmt.Println("Error Opening DB")
		return
	}

	lastID, err := sqlite.GetLastId(db)
	if err != nil || lastID == 0 {
		fmt.Println("Error Querying max ID of jsonindex")
		return
	}

	systems := make(map[string]string, 0)
	regions := make(map[string]string, 0)
	languages := make(map[string]string, 0)
	publishers := make(map[string]string, 0)
	developers := make(map[string]string, 0)
	genres := make(map[string]string, 0)
	franchises := make(map[string]string, 0)
	exts := make(map[string]string, 0)
	uniqueTypes := make(map[string]string, 0)
	titles := make(map[string]string, 0)

	for _, system := range rdb.RBDNames {
		systems[system] = system
	}

	seedRegions := []string{
		// NOINTRO
		"world", "europe", "asia", "australia", "brazil", "canada", "china", "france",
		"germany", "hong kong", "italy", "japan", "korea", "netherlands", "spain",
		"sweden", "usa", "poland", "finland", "denmark", "portugal", "norway",
		// TOSEC
		"AE", "AL", "AS", "AT", "AU", "BA", "BE", "BG", "BR", "CA", "CH", "CL", "CN",
		"CS", "CY", "CZ", "DE", "DK", "EE", "EG", "ES", "EU", "FI", "FR", "GB", "GR",
		"HK", "HR", "HU", "ID", "IE", "IL", "IN", "IR", "IS", "IT", "JO", "JP", "KR",
		"LT", "LU", "LV", "MN", "MX", "MY", "NL", "NO", "NP", "NZ", "OM", "PE", "PH",
		"PL", "PT", "QA", "RO", "RU", "SE", "SG", "SI", "SK", "TH", "TR", "TW", "US",
		"VN", "YU", "ZA",
	}
	for _, region := range seedRegions {
		regions[region] = region
	}

	seedLanguages := []string{
		"ar", "bg", "bs", "cs", "cy", "da", "de", "el", "en", "eo", "es", "et",
		"fa", "fi", "fr", "ga", "gu", "he", "hi", "hr", "hu", "is", "it", "ja",
		"ko", "lt", "lv", "ms", "nl", "no", "pl", "pt", "ro", "ru", "sk", "sl",
		"sq", "sr", "sv", "th", "tr", "ur", "vi", "yi", "zh",
	}
	for _, lang := range seedLanguages {
		languages[lang] = lang
	}

	for id := 1; id <= lastID; id++ {
		row, err := sqlite.GetJsonIndexRow(db, id)
		if err != nil {
			fmt.Println("Error Fetching row id", id)
			continue
		}

		v, err := rdb.UnmarshalRomJson(row.RDBJson)
		if err != nil {
			fmt.Println("Error Unmarshalling JSON", id)
			continue
		}

		frag := ztdb.GetFileFragments(v.RomName)

		if _, ok := systems[v.RDBName]; !ok && v.RDBName != "" {
			systems[v.RDBName] = v.RDBName
		}
		if _, ok := regions[v.Region]; !ok && v.Region != "" {
			regions[v.Region] = v.Region
		}
		// language := ""
		// if _, ok := languages[language]; !ok && language != ""{
		// 	languages[language] = language
		// }
		if _, ok := publishers[v.Publisher]; !ok && v.Publisher != "" {
			publishers[v.Publisher] = v.Publisher
		}
		if _, ok := developers[v.Developer]; !ok && v.Developer != "" {
			developers[v.Developer] = v.Developer
		}
		if _, ok := genres[v.Genre]; !ok && v.Genre != "" {
			genres[v.Genre] = v.Genre
		}
		if _, ok := franchises[v.Franchise]; !ok && v.Franchise != "" {
			franchises[v.Franchise] = v.Franchise
		}
		ext := frag.Ext
		if _, ok := exts[ext]; !ok && ext != "" {
			exts[ext] = ext
		}
		if _, ok := uniqueTypes[row.UniqueType]; !ok && row.UniqueType != "" {
			uniqueTypes[row.UniqueType] = row.UniqueType
		}
		tName := v.Name
		if tName == "" {
			tName = frag.FileNameNoExt
		}
		title := ztdb.GetTitleFromName(tName)
		if _, ok := titles[title]; !ok && title != "" {
			titles[title] = title
		}
	}

	saveMetaNDJSON(systems, sqlite.TableSystem, func(i int, metaStr string) any {
		return ztdb.System{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(regions, sqlite.TableRegion, func(i int, metaStr string) any {
		return ztdb.Region{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(languages, sqlite.TableLanguage, func(i int, metaStr string) any {
		return ztdb.Language{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(publishers, sqlite.TablePublisher, func(i int, metaStr string) any {
		return ztdb.Publisher{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(developers, sqlite.TableDeveloper, func(i int, metaStr string) any {
		return ztdb.Developer{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(genres, sqlite.TableGenre, func(i int, metaStr string) any {
		return ztdb.Genre{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(franchises, sqlite.TableFranchise, func(i int, metaStr string) any {
		return ztdb.Franchise{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(exts, sqlite.TableFileExtension, func(i int, metaStr string) any {
		return ztdb.FileExtension{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(uniqueTypes, sqlite.TableUniqueType, func(i int, metaStr string) any {
		return ztdb.UniqueType{
			ID:   i,
			Name: metaStr,
		}
	})
	saveMetaNDJSON(titles, sqlite.TableTitle, func(i int, metaStr string) any {
		return ztdb.Title{
			ID:   i,
			Name: metaStr,
		}
	})
}

func saveMetaNDJSON(metas map[string]string, name string, cb func(int, string) any) {
	metasS := make([]string, 0)
	for metaStr := range metas {
		metasS = append(metasS, metaStr)
	}
	sort.Strings(metasS)
	ndjsonPath := filepath.Join(settings.DBJsonDir, fmt.Sprintf("_%v.ndjson", name))
	outfile, err := os.Create(ndjsonPath)
	if err != nil {
		fmt.Println("Cannot create NDJSON", ndjsonPath)
		return
	}
	defer outfile.Close()
	for i, metaStr := range metasS {
		meta := cb(i+1, metaStr)
		b, err := json.Marshal(meta)
		if err != nil {
			fmt.Println()
		}
		outfile.Write(b)
		outfile.WriteString("\n")
	}
}

func makeztdbjsonvariants() {
	// Load all meta NDJSON into memory DB
	genericTables := []string{
		sqlite.TableRegion,
		sqlite.TableLanguage,
		sqlite.TablePublisher,
		sqlite.TableDeveloper,
		sqlite.TableGenre,
		sqlite.TableFranchise,
		sqlite.TableFileExtension,
		sqlite.TableUniqueType,
		sqlite.TableTitle,
	}

	db, err := sqlite.OpenMemoryZTDB()
	if err != nil {
		fmt.Println("Unable to Open memory DB", err)
		return
	}

	systems, err := ztdb.LoadNDJSON(sqlite.TableSystem, make([]ztdb.System, 0))
	{
		table := sqlite.TableSystem
		if err != nil {
			fmt.Println("Unable to load ndjson", table, err)
		}
		err = sqlite.BulkInsertSystems(db, systems)
		if err != nil {
			fmt.Println("Error BulkInserting into", table, err)
		}
	}
	for _, table := range genericTables {
		metas, err := ztdb.LoadNDJSON(table, make([]ztdb.GenericDBMeta, 0))
		if err != nil {
			fmt.Println("Unable to load ndjson", table, err)
		}
		err = sqlite.BulkInsertGenericMeta(db, table, metas)
		if err != nil {
			fmt.Println("Error BulkInserting into", table, err)
		}
	}

	udb, err := sqlite.OpenUniqueDB()
	if err != nil {
		fmt.Println("Error Opening Unique DB", err)
		return
	}

	lastID, err := sqlite.GetLastId(udb)
	if err != nil || lastID == 0 {
		fmt.Println("Error Querying max ID of jsonindex", err)
		return
	}

	db.Exec(`BEGIN`)
	for id := 1; id <= lastID; id++ {
		row, err := sqlite.GetJsonIndexRow(udb, id)
		if err != nil {
			fmt.Println("Error Fetching row id", id, err)
			continue
		}

		v, err := rdb.UnmarshalRomJson(row.RDBJson)
		if err != nil {
			fmt.Println("Error Unmarshalling JSON", id, err)
			continue
		}

		frag := ztdb.GetFileFragments(v.RomName)
		ext := frag.Ext
		tName := v.Name
		if tName == "" {
			tName = frag.FileNameNoExt
		}
		title := ztdb.GetTitleFromName(tName)

		tv := ztdb.TitleVariant{
			ID:           row.ID,
			Filename:     v.RomName,
			ReleaseYear:  v.ReleaseYear,
			ReleaseMonth: v.ReleaseMonth,
			Users:        v.Users,
			Serial:       v.Serial,
			MD5:          v.MD5,
			SHA1:         v.SHA1,
			CRC:          v.CRC,
			Size:         v.Size,
			Name:         v.Name,
			Description:  v.Description,
		}

		// Clear redundant names/descriptions for storage
		if tv.Name == tv.Filename || tv.Name == frag.FileNameNoExt {
			tv.Name = ""
		}
		if tv.Description == tv.Filename || tv.Description == frag.FileNameNoExt || tv.Description == tv.Name {
			tv.Description = ""
		}

		if title != "" {
			table := sqlite.TableTitle
			name := title
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.TitleID = id
		}
		if v.RDBName != "" {
			table := sqlite.TableSystem
			name := v.RDBName
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.SystemID = id
		}
		if v.Region != "" {
			table := sqlite.TableRegion
			name := v.Region
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.RegionID = id
		}
		if v.Publisher != "" {
			table := sqlite.TablePublisher
			name := v.Publisher
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.PublisherID = id
		}
		if v.Developer != "" {
			if v.Developer == "&lt;unknown&gt;" {
				v.Developer = "<unknown>"
			}
			table := sqlite.TableDeveloper
			name := v.Developer
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.DeveloperID = id
		}
		if v.Genre != "" {
			table := sqlite.TableGenre
			name := v.Genre
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.GenreID = id
		}
		if v.Franchise != "" {
			table := sqlite.TableFranchise
			name := v.Franchise
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.FranchiseID = id
		}
		if ext != "" {
			table := sqlite.TableFileExtension
			name := ext
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.ExtensionID = id
		}
		if v.UniqueType != "" {
			table := sqlite.TableUniqueType
			name := row.UniqueType
			id, err := sqlite.GetMetaNameID(db, table, name)
			if err != nil {
				fmt.Println("Error getting meta id for", table, name, err)
			}
			tv.UniqueTypeID = id
		}

		err = sqlite.InsertTitleVariants(db, tv)
		if err != nil {
			fmt.Println("Error inserting Title Variant", err)
			fmt.Printf("%+v", tv)
			return
		}
	}
	db.Exec(`Commit`)
	db.Exec("VACUUM INTO ?", settings.DBPath)

	// Generate the NDJSONs by SYSTEM ID
	for _, system := range systems {
		tvs, err := sqlite.GetTitleVariantsBySystemID(db, system.ID)
		if err != nil || len(tvs) == 0 {
			fmt.Println("error searching TitleVariants by SystemID", err)
			continue
		}

		ndjsonPath := filepath.Join(settings.DBJsonDir, fmt.Sprintf("%v.ndjson", system.Name))
		outfile, err := os.Create(ndjsonPath)
		if err != nil {
			fmt.Println("Cannot create NDJSON", ndjsonPath, err)
			continue
		}

		for _, tv := range tvs {
			b, err := json.Marshal(tv)
			if err != nil {
				fmt.Println("error writing NDJSON", system.Name, tv.ID, err)
			}
			outfile.Write(b)
			outfile.WriteString("\n")
		}
		outfile.Close()
	}

	udb.Close()
	db.Close()
}
