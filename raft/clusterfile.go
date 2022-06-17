package raft

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"
)

func readMetadataFromFile(filepath string) (*RaftMetadata, error) {
	if !fileExist(filepath) {
		return nil, os.ErrNotExist
	}

	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	meta := &RaftMetadata{}
	err = json.Unmarshal(b, meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func writeMetadataToFile(filepath string, meta *RaftMetadata) error {
	b, _ := json.MarshalIndent(meta, "", "    ")

	return ioutil.WriteFile(filepath, b, 0644)
}

func readClusterFromFile(filepath string) (*gossip.RaftClusterMessage, error) {
	if !fileExist(filepath) {
		return nil, os.ErrNotExist
	}

	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	cluster := &gossip.RaftClusterMessage{}
	err = json.Unmarshal(b, cluster)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func writeClusterToFile(filepath string, cluster *gossip.RaftClusterMessage) error {
	b, _ := json.MarshalIndent(cluster, "", "    ")

	return ioutil.WriteFile(filepath, b, 0644)
}

func fileExist(filepath string) bool {
	_, err := os.Lstat(filepath)
	return !os.IsNotExist(err)
}

func pathIsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return true
}
