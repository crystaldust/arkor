package inner

import (
	"encoding/json"
	// "fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"gopkg.in/macaron.v1"

	"github.com/containerops/arkor/models"
	"github.com/containerops/arkor/utils"
	"github.com/containerops/arkor/utils/db"
	"github.com/jinzhu/gorm"
)

func PutDataserverHandler(ctx *macaron.Context, req models.DataServer, log *logrus.Logger) (int, []byte) {
	ds := &models.DataServer{
		IP:   req.IP,
		Port: req.Port,
	}

	// Query DataServer ID from SQL Database
	if exist, err := db.SQLDB.Query(ds); !exist && err == nil {
		return http.StatusNotFound, []byte("Data server is NOT registered")
	} else if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	req.ID = ds.ID
	req.UpdateTime = time.Now()
	// Save dataserver status to K/V Database
	if err := db.KVDB.Save(&req); err != nil {
		log.Errorln(err.Error())
		return http.StatusInternalServerError, []byte(err.Error())
	}

	// Save dataserver status to SQL Database
	if err := db.SQLDB.Save(&req); err != nil {
		log.Errorln(err.Error())
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, nil
}

func AddDataserverHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	data, _ := ctx.Req.Body().Bytes()
	dataServers := []models.DataServer{}
	if err := json.Unmarshal(data, &dataServers); err != nil || len(dataServers) == 0 {
		return http.StatusBadRequest, []byte("Invalid Parameters or Incorrect json content")
	}

	groupServerMap := make(map[string][]models.DataServer)
	resultArray := []map[string]interface{}{}

	for i := range dataServers {
		dataServer := dataServers[i]

		now := time.Now()
		dataServer.DataServerID = utils.MD5ID()
		dataServer.CreateTime = now
		dataServer.UpdateTime = now

		groupID := dataServer.GroupID
		_, exists := groupServerMap[groupID]
		if exists == true {
			groupServerMap[groupID] = append(groupServerMap[groupID], dataServer)
		} else {
			groupServerMap[groupID] = []models.DataServer{dataServer}
		}

		ds := make(map[string]interface{})
		ds["group_id"] = dataServer.GroupID
		ds["ip"] = dataServer.IP
		ds["port"] = dataServer.Port
		ds["data_server_id"] = dataServer.DataServerID
		resultArray = append(resultArray, ds)
	}

	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	errorOccur := make(chan error)

	var wg sync.WaitGroup
	wg.Add(len(groupServerMap))
	for groupID, servers := range groupServerMap {
		go func(gid string, servers []models.DataServer) {
			defer wg.Done()
			group := models.Group{ID: gid}
			found, _ := db.SQLDB.Query(&group)
			var err error
			if found == false { // Create the group
				err = dbInstance.Create(&models.Group{
					ID:      gid,
					Servers: servers,
				}).Error
			} else { // Append the servers to the group
				err = dbInstance.Model(&group).Association("Servers").Append(servers).Error
			}

			if err != nil {
				errorOccur <- err
			}
		}(groupID, servers)
	}

	go func() { // If everything is going ok, errorOccur won't receive any error and is closed
		wg.Wait()
		close(errorOccur)
	}()

	// TODO The code is vulnerable!
	// The goroutines above may produce mutilple errors, while We currently try to handle the first one
	// What's more, we can't skip the useless goroutine by checking if other goroutines catch errors, since
	// the judege is too early for a database operation
	err := <-errorOccur
	if err != nil {
		log.Errorln(err)
		if strings.Contains(err.Error(), "1062") {
			return http.StatusConflict, []byte("Data Server already registered")
		} else {
			return http.StatusInternalServerError, nil
		}
	}

	result, _ := json.Marshal(resultArray)
	ctx.Resp.Header().Set("Content-Type", "application/json")
	return http.StatusOK, result
}

func DeleteDataserverHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	dataserverID := ctx.Params(":dataserver")
	ds := &models.DataServer{
		DataServerID: dataserverID,
	}

	// if err := db.KVDB.Delete(ds); err != nil {
	// 	return http.StatusInternalServerError, []byte(err.Error())
	// }

	var count int
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	dbInstance.Where(&ds).Find(&ds).Count(&count)

	if count == 0 {
		return http.StatusNotFound, []byte("Data server is NOT registered")
	}

	if err := db.SQLDB.Delete(ds); err != nil {
		log.Errorln(err.Error())
		return http.StatusInternalServerError, []byte("Internal Server Error")
	}

	return http.StatusOK, nil
}

func GetDataserverHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	dataserverID := ctx.Params(":dataserver")
	ds := &models.DataServer{
		DataServerID: dataserverID,
	}

	// exist, err := db.KVDB.Query(ds)
	// if exist && err == nil { // Got the server from cache
	// 	result, _ := json.Marshal(ds)
	// 	ctx.Resp.Header().Set("Content-Type", "application/json")
	// 	return http.StatusOK, result
	// }

	// If there is no info in cache, try to fetch it from SQLDB and rebuild the cache
	if exist, err := db.SQLDB.Query(ds); !exist && err == nil {
		return http.StatusNotFound, []byte("Data server is NOT registered")
	} else if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	// db.KVDB.Create(ds)

	ctx.Resp.Header().Set("Content-Type", "application/json")
	result, _ := json.Marshal(ds)
	return http.StatusOK, result
}

func GetGroupsHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)

	var groups []models.Group
	if err := dbInstance.Preload("Servers").Find(&groups).Error; err != nil {
		log.Errorln(err)
		return http.StatusInternalServerError, []byte("Internal Server Error")
	}

	ctx.Resp.Header().Set("Content-Type", "application/json")
	result, _ := json.Marshal(groups)

	return http.StatusOK, result
}

func GetGroupHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	groupID := ctx.Params(":group")
	group := models.Group{
		ID: groupID,
	}

	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	if err := dbInstance.Preload("Servers").Find(&group).Error; err != nil {
		log.Errorln(err)
		return http.StatusInternalServerError, []byte("Internal Server Error")
	}

	result, _ := json.Marshal(&group)

	ctx.Resp.Header().Set("Content-Type", "application/json")
	return http.StatusOK, result
}
