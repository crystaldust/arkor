package models

import (
	"time"

	"github.com/containerops/arkor/utils/db/mysql"
	"github.com/jinzhu/gorm"
)

type ObjectMeta struct {
	gorm.Model
	Content
	ObjectID  string     `json:"object_id,omitempty" gorm:"column:object_id"`
	Key       string     `json:"object_key,omitempty" gorm:"column:object_key"`
	Md5Key    string     `json:"md5_key,omitempty" gorm:"column:md5_key`
	Fragments []Fragment `json:"fragments,omitempty" gorm:"ForeignKey:ObjectMetaID"`
}

type Fragment struct {
	gorm.Model   `json:"-"`
	ObjectMetaID string    `json:"-"`
	FragmentID   string    `json:"id" gorm:"column:fragment_id"`
	Index        int       `json:"index" gorm:"column:index"`
	Start        int64     `json:"start"`
	End          int64     `json:"end"`
	GroupID      string    `json:"group_id"`
	FileID       string    `json:"file_id"`
	IsLast       bool      `json:"is_last"`
	ModTime      time.Time `json:"mod_time"`
}

type FragIDConvert struct {
	FragIDstr string `json:"fragIDstr" gorm:"unique"`
	FragIDint int64  `json:"fragIDstr" gorm:"primary_key;AUTO_INCREMENT"`
}

func (objectMeta *ObjectMeta) Associate() {
	mysqldb := mysql.MySQLInstance()
	mysqldb.Model(&ObjectMeta{}).Related(&Fragment{}, "Fragments")
}
