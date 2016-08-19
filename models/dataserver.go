package models

import (
	"time"

	"github.com/jinzhu/gorm"
)

// status of Data Server
const (
	RW_STATUS  = 0
	RO_STATUS  = 1
	ERR_STATUS = 2
)

// struct of DataServer
type DataServer struct {
	gorm.Model     `json:"-"`
	DataServerID   string    `json:"data_server_id,omitempty" gorm:"unique;"`
	GroupID        string    `json:"group_id,omitempty" gorm:"unique_index:gid_ip_port" binding:"Required"`
	IP             string    `json:"ip,omitempty" gorm:"unique_index:gid_ip_port" binding:"Required"`
	Port           int       `json:"port,omitempty" gorm:"unique_index:gid_ip_port" binding:"Required"`
	Status         int       `json:"status,omitempty"`
	Deleted        int       `json:"deleted,omitempty"`
	TotalChunks    int       `json:"total_chunks,omitempty"`
	TotalFreeSpace int64     `json:"total_free_space,omitempty"`
	MaxFreeSpace   int64     `json:"max_free_space,omitempty"`
	DataPath       string    `json:"data_path,omitempty"`
	PendingWrites  int       `json:"pend_writes,omitempty"`
	ReadingCount   int64     `json:"reading_count,omitempty"`
	ConnCounts     int       `json:"conn_counts,omitempty"`
	CreateTime     time.Time `json:"create_time,omitempty"`
	UpdateTime     time.Time `json:"update_time,omitempty"`
}

func (ds *DataServer) PK() string {
	return "DataServerID"
}
