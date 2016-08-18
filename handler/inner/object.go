package inner

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/jinzhu/gorm"
	"gopkg.in/macaron.v1"

	"github.com/containerops/arkor/models"
	"github.com/containerops/arkor/utils"
	"github.com/containerops/arkor/utils/db"
)

func AllocateFileID(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	m := make(map[string]string)
	m["file_id"] = utils.MD5ID()

	result, err := json.Marshal(m)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, result
}

func PutObjectInfoHandler(ctx *macaron.Context, req models.ObjectMeta, log *logrus.Logger) (int, []byte) {
	if len(req.Fragments) == 0 {
		return http.StatusBadRequest, []byte("Invalid Parameters")
	}

	for i := range req.Fragments {
		req.Fragments[i].FragmentID = utils.MD5ID()
	}

	// Find the record in db
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	var objectMeta models.ObjectMeta
	dbInstance.Where("id = ?", req.ID).First(&objectMeta)

	var err error
	if &objectMeta == nil || objectMeta.ID == "" { // Create new record
		err = db.SQLDB.Create(&req)
	} else { // Update the objectMeta
		err = dbInstance.Model(&objectMeta).Association("Fragments").Replace(req.Fragments).Error
	}

	if err != nil {
		log.Errorln(err.Error())
		return http.StatusInternalServerError, nil
	}

	return http.StatusOK, nil
}

func GetObjectInfoHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	objectID := ctx.Params(":object")

	sql := fmt.Sprintf(
		"SELECT object.object_id, object.fragment_id, object_key, md5_key, `index`, start, end, group_id, file_id, is_last, mod_time FROM object, object_meta, fragment WHERE object_meta.id=%q AND object.object_id=%q  AND fragment.id=object.fragment_id",
		objectID, objectID)

	dbInstace := db.SQLDB.GetDB().(*gorm.DB)

	rows, err := dbInstace.Raw(sql).Rows()
	if err != nil {
		return http.StatusInternalServerError, []byte("Internal Server Error")
	}

	defer rows.Close()

	object := make(map[string]interface{})
	fragments := []interface{}{}
	for rows.Next() {
		var object_id string
		var fragment_id string
		var object_key string
		var md5_key string
		var index int
		var start int64
		var end int64
		var group_id string
		var file_id string
		var is_last bool
		var mod_time string

		rows.Scan(&object_id, &fragment_id, &object_key, &md5_key, &index, &start, &end, &group_id, &file_id, &is_last, &mod_time)

		object["object_id"] = object_id
		object["md5_key"] = md5_key
		object["object_key"] = object_key

		fragment := make(map[string]interface{})
		fragment["index"] = index
		fragment["start"] = start
		fragment["end"] = end
		fragment["group_id"] = group_id
		fragment["file_id"] = file_id
		fragment["is_last"] = is_last
		if mod_time == "" {
			mod_time = time.Now().Format("2006-01-02T15:04:05Z07:00")
		}
		fragment["mod_time"] = mod_time

		fragments = append(fragments, fragment)
	}
	object["fragments"] = fragments

	result, err := json.Marshal(object)

	if err != nil {
		log.Errorln(err.Error())
		return http.StatusInternalServerError, []byte("Internal Server Error")

	}
	ctx.Resp.Header().Set("Content-Type", "application/json")
	return http.StatusOK, result
}
