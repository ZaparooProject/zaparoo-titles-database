package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/ZaparooProject/zaparoo-titles-database/pkg/settings"
	"github.com/ZaparooProject/zaparoo-titles-database/pkg/ztdb"
	_ "github.com/mattn/go-sqlite3"
)

const (
	TableSystem        string = "Systems"
	TableRegion        string = "Regions"
	TableLanguage      string = "Languages"
	TablePublisher     string = "Publishers"
	TableDeveloper     string = "Developers"
	TableGenre         string = "Genres"
	TableFranchise     string = "Franchises"
	TableFileExtension string = "FileExtensions"
	TableUniqueType    string = "UniqueTypes"
	TableTitle         string = "Titles"
	TableTitleVariant  string = "TitleVariants"
)

func OpenVariantIndexDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	sqlStmt := `
		CREATE TABLE uniqueindex (
			UniqueKey TEXT PRIMARY KEY,
			UniqueType TEXT,
			Filename TEXT,
			RDBJson TEXT
		);

		CREATE TABLE jsonindex (
			ID INTEGER PRIMARY KEY,
			UniqueKey TEXT,
			UniqueType TEXT,
			Filename TEXT,
			RDBJson TEXT
		);
	`
	_, err = db.Exec(sqlStmt)
	return db, err
}

type JsonIndexRow struct {
	ID         int
	UniqueType string
	Filename   string
	RDBJson    string
}

func IndexUnique(db *sql.DB, uniqueKey string, uniqueType string, filename string, rdbJson string) error {
	if uniqueKey == "" {
		return fmt.Errorf("uniqueKey empty, trying next key type")
	}
	sqlStmt := `
		INSERT INTO uniqueindex
		(UniqueKey, UniqueType, Filename, RDBJson)
		VALUES
		(?, ?, ?, ?);
	`
	_, err := db.Exec(sqlStmt, uniqueKey, uniqueType, filename, rdbJson)
	return err
}

func ReindexByFilename(db *sql.DB) error {
	_, err := db.Exec(`
		INSERT INTO jsonindex 
		(UniqueKey, UniqueType, Filename, RDBJson) 
		SELECT
		UniqueKey, UniqueType, Filename, RDBJson
		FROM uniqueindex ORDER BY Filename ASC;

		DROP TABLE uniqueindex;
	`)
	return err
}

func OpenUniqueDB() (*sql.DB, error) {
	return sql.Open("sqlite3", settings.DBSqliteUniquePath)
}

func GetLastId(db *sql.DB) (int, error) {
	var maxID int
	q, err := db.Prepare(`
		SELECT
		MAX(ID)
		FROM jsonindex;
	`)
	if err != nil {
		return 0, err
	}
	defer q.Close()
	err = q.QueryRow().Scan(&maxID)
	return maxID, err
}

func GetJsonIndexRow(db *sql.DB, id int) (JsonIndexRow, error) {
	var row JsonIndexRow
	q, err := db.Prepare(`
		SELECT
		ID, UniqueType, Filename, RDBJson
		FROM jsonindex
		WHERE ID = ?;
	`)
	if err != nil {
		return row, err
	}
	defer q.Close()
	err = q.QueryRow(id).Scan(
		&row.ID,
		&row.UniqueType,
		&row.Filename,
		&row.RDBJson,
	)
	return row, err
}

func OpenMemoryZTDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	sqlStmt := `
		CREATE TABLE ZTDBInfo (
			Version TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		INSERT INTO ZTDBInfo
		(Version, Description)
		VALUES
		("1.0", "Initial Build");

		CREATE TABLE Systems (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			ZaparooSystemID TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Regions (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Languages (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Publishers (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Developers (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Genres (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Franchises (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE FileExtensions (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE UniqueTypes (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE Titles (
			ID INTEGER PRIMARY KEY,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);

		CREATE TABLE TitleVariants (
			ID INTEGER PRIMARY KEY,
			TitleID INTEGER NOT NULL,
			SystemID INTEGER NOT NULL,
			Filename TEXT NOT NULL,
			ReleaseYear INTEGER NOT NULL,
			ReleaseMonth INTEGER NOT NULL,
			Users INTEGER NOT NULL,
			RegionID INTEGER NOT NULL,
			PublisherID INTEGER NOT NULL,
			DeveloperID INTEGER NOT NULL,
			GenreID INTEGER NOT NULL,
			FranchiseID INTEGER NOT NULL,
			ExtensionID INTEGER NOT NULL,
			UniqueTypeID INTEGER NOT NULL,
			Serial TEXT NOT NULL,
			MD5 TEXT NOT NULL,
			SHA1 TEXT NOT NULL,
			CRC TEXT NOT NULL,
			Size INTEGER NOT NULL,
			Name TEXT NOT NULL,
			Description TEXT NOT NULL
		);
	`
	_, err = db.Exec(sqlStmt)
	return db, err
}

func BulkInsertSystems(db *sql.DB, systems []ztdb.System) error {
	db.Exec(`BEGIN`)
	for _, system := range systems {
		_, err := db.Exec(`
			INSERT INTO Systems
			(ID, Name, ZaparooSystemID, Description)
			VALUES
			(?, ?, ?, ?);
		`, system.ID, system.Name, system.ZaparooSystemID, system.Description)
		if err != nil {
			return err
		}
	}
	db.Exec(`COMMIT`)
	return nil
}

func BulkInsertGenericMeta(db *sql.DB, table string, metas []ztdb.GenericDBMeta) error {
	db.Exec(`BEGIN`)
	for _, meta := range metas {
		_, err := db.Exec(`
			INSERT INTO `+table+`
			(ID, Name, Description)
			VALUES
			(?, ?, ?);
		`, meta.ID, meta.Name, meta.Description)
		if err != nil {
			return err
		}
	}
	db.Exec(`COMMIT`)
	return nil
}

func InsertTitleVariants(db *sql.DB, s ztdb.TitleVariant) error {
	_, err := db.Exec(`
		INSERT INTO TitleVariants
		(ID, TitleID, SystemID, Filename, ReleaseYear, ReleaseMonth, Users, RegionID, PublisherID, DeveloperID,
		GenreID, FranchiseID, ExtensionID, UniqueTypeID, Serial, MD5, SHA1, CRC, Size, Name, Description)
		VALUES
		(
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		);
		`, s.ID, s.TitleID, s.SystemID, s.Filename, s.ReleaseYear, s.ReleaseMonth, s.Users, s.RegionID, s.PublisherID, s.DeveloperID,
		s.GenreID, s.FranchiseID, s.ExtensionID, s.UniqueTypeID, s.Serial, s.MD5, s.SHA1, s.CRC, s.Size, s.Name, s.Description)
	return err
}

func GetMetaNameID(db *sql.DB, table string, name any) (int, error) {
	var id int
	q, err := db.Prepare(`
		SELECT
		ID
		FROM ` + table + `
		WHERE Name = ?;
	`)
	if err != nil {
		return id, err
	}
	defer q.Close()
	err = q.QueryRow(name).Scan(&id)
	return id, err
}

func GetTitleVariantsBySystemID(db *sql.DB, systemID int) ([]ztdb.TitleVariant, error) {
	var results []ztdb.TitleVariant
	stmt, err := db.Prepare(`
		SELECT
		ID, TitleID, SystemID, Filename,ReleaseYear, ReleaseMonth, Users, RegionID, PublisherID, DeveloperID,
		GenreID, FranchiseID, ExtensionID, UniqueTypeID, Serial, MD5, SHA1, CRC, Size, Name, Description
		FROM TitleVariants
		WHERE SystemID = ?
	`)
	rows, err := stmt.Query(systemID)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	for rows.Next() {
		s := ztdb.TitleVariant{}
		err := rows.Scan(
			&s.ID, &s.TitleID, &s.SystemID, &s.Filename, &s.ReleaseYear, &s.ReleaseMonth, &s.Users, &s.RegionID, &s.PublisherID, &s.DeveloperID,
			&s.GenreID, &s.FranchiseID, &s.ExtensionID, &s.UniqueTypeID, &s.Serial, &s.MD5, &s.SHA1, &s.CRC, &s.Size, &s.Name, &s.Description,
		)
		if err != nil {
			return results, err
		}
		results = append(results, s)
	}
	err = rows.Err()
	if err != nil {
		return results, err
	}
	return results, nil
}
