package ztdb

import (
	"path/filepath"
	"regexp"
	"strings"
)

type FileFragments struct {
	FileNameNoExt string
	Title         string
	Ext           string
	//Tags     []string
}

func GetTitleFromName(filename string) string {
	if filename == "" {
		return filename
	}
	r := regexp.MustCompile(`^([^\(\[]*)`)
	title := r.FindString(filename)
	return strings.TrimSpace(title)
}

func GetTagsFromFileName(filename string) []string {
	re := regexp.MustCompile(`\(([\w\,\- ]*)\)|\[([\w\,\- ]*)\]`)
	matches := re.FindAllString(filename, -1)
	tags := make([]string, 0)
	for _, padded := range matches {
		unpadded := padded[1 : len(padded)-1]
		split := strings.Split(unpadded, ",")
		for _, tag := range split {
			tags = append(tags, strings.ToLower(strings.TrimSpace(tag)))
		}
	}
	return tags
}

func GetFileFragments(filename string) FileFragments {
	f := FileFragments{}
	filename = strings.Trim(filename, " ")
	f.Ext = strings.ToLower(filepath.Ext(filename))
	f.FileNameNoExt, _ = strings.CutSuffix(filename, f.Ext)
	f.Title = GetTitleFromName(f.FileNameNoExt)
	//f.Tags = getTagsFromFileName(f.FileName)
	return f
}
