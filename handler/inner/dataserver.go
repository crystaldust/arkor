package inner

import (
	"encoding/json"
	"fmt"
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
		serverID := utils.MD5ID()
		dataServer.DataServerID = serverID
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

func AddDataserverHandler2(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	data, _ := ctx.Req.Body().Bytes()
	dataServers := []models.DataServer{}
	if err := json.Unmarshal(data, &dataServers); err != nil || len(dataServers) == 0 {
		return http.StatusBadRequest, []byte("Invalid Parameters or Incorrect json content")
	}

	checkDataServerFormat := "SELECT COUNT(*) FROM data_server WHERE ip IN (%s) AND port IN (%s) AND group_id IN (%s)"
	checkIPs := ""
	checkPorts := ""
	checkGroupIDs := ""

	insertDataServerSql := "INSERT INTO data_server (id, group_id, ip, port, create_time, update_time) VALUES "
	insertGroupServerSql := "INSERT INTO group_server (group_id, server_id) VALUES "

	resultAry := []interface{}{}

	for i := range dataServers {
		dataServer := dataServers[i]

		now := time.Now()
		nowStr := now.Format("2006-01-02 15:04:05") // I don't know what the writer of Go is thinking of!
		serverID := utils.MD5ID()
		dataServer.DataServerID = serverID
		dataServer.CreateTime = now
		dataServer.UpdateTime = now

		insertDataServer := fmt.Sprintf("(%q, %q, %q, %d, %q, %q),", serverID, dataServer.GroupID, dataServer.IP, dataServer.Port, nowStr, nowStr)
		insertGroupServer := fmt.Sprintf("(%q, %q),", dataServer.GroupID, serverID)

		insertDataServerSql += insertDataServer
		insertGroupServerSql += insertGroupServer

		checkIPs += fmt.Sprintf("%q,", dataServer.IP)
		checkPorts += fmt.Sprintf("%d,", dataServer.Port)
		checkGroupIDs += fmt.Sprintf("%q,", dataServer.GroupID)

		dsObj := make(map[string]interface{})
		dsObj["ip"] = dataServer.IP
		dsObj["port"] = dataServer.Port
		dsObj["group_id"] = dataServer.GroupID
		dsObj["data_server_id"] = serverID

		resultAry = append(resultAry, dsObj)
	}

	dbInstance := db.SQLDB.GetDB().(*gorm.DB)

	checkIPs = checkIPs[:len(checkIPs)-1]
	checkPorts = checkPorts[:len(checkPorts)-1]
	checkGroupIDs = checkGroupIDs[:len(checkGroupIDs)-1]
	checkExistenceSql := fmt.Sprintf(checkDataServerFormat, checkIPs, checkPorts, checkGroupIDs)
	var cnt int
	err := dbInstance.Raw(checkExistenceSql).Row().Scan(&cnt)
	if err != nil {
		return http.StatusInternalServerError, []byte("Internal server error")
	}
	if cnt > 0 {
		return http.StatusConflict, []byte("Conflict (Data Server already registered)")
	}

	// Remove the last ','
	insertDataServerSql = insertDataServerSql[:len(insertDataServerSql)-1]
	insertGroupServerSql = insertGroupServerSql[:len(insertGroupServerSql)-1]

	if result := dbInstance.Exec(insertDataServerSql); result.Error != nil {
		log.Println(result.Error)
		return http.StatusInternalServerError, []byte(result.Error.Error())
	}

	if result := dbInstance.Exec(insertGroupServerSql); result.Error != nil {
		log.Println(result.Error)
		return http.StatusInternalServerError, []byte(result.Error.Error())
	}

	// Cache the data server info
	numCached := 0
	numServers := len(dataServers)
	cached := make(chan bool)
	for _, dataServer := range dataServers {
		go func() {
			// Save dataserver info to K/V Database as cache
			if err := db.KVDB.Create(&dataServer); err != nil {
				log.Println(err.Error())
				cached <- false
				return
			}
			numCached++
			if numCached >= numServers {
				cached <- true
			}
		}()
	}

	if <-cached == false {
		return http.StatusInternalServerError, []byte("Internal server error")
	}

	ctx.Resp.Header().Set("Content-Type", "application/json")
	result, _ := json.Marshal(resultAry)
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

	exist, err := db.KVDB.Query(ds)
	if exist && err == nil { // Got the server from cache
		result, _ := json.Marshal(ds)
		ctx.Resp.Header().Set("Content-Type", "application/json")
		return http.StatusOK, result
	}

	// If there is no info in cache, try to fetch it from SQLDB and rebuild the cache
	if exist, err := db.SQLDB.Query(ds); !exist && err == nil {
		return http.StatusNotFound, []byte("Data server is NOT registered")
	} else if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	db.KVDB.Create(ds)

	ctx.Resp.Header().Set("Content-Type", "application/json")
	result, _ := json.Marshal(ds)
	return http.StatusOK, result
}

func GetGroupsHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	rows, err := dbInstance.Raw("SELECT * FROM data_server, group_server WHERE data_server.id=group_server.server_id").Rows()
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	defer rows.Close()

	groupMap := make(map[string]interface{})

	for rows.Next() {
		var gsInfo models.GroupServerInfo
		dbInstance.ScanRows(rows, &gsInfo)

		server := make(map[string]interface{})
		server["data_server_id"] = gsInfo.ServerID
		server["ip"] = gsInfo.IP
		server["port"] = gsInfo.Port
		server["status"] = gsInfo.Status
		server["group_status"] = gsInfo.GroupStatus
		server["total_chunks"] = gsInfo.TotalChunks
		server["total_free_space"] = gsInfo.TotalFreeSpace
		server["max_free_space"] = gsInfo.MaxFreeSpace
		server["pending_writes"] = gsInfo.PendingWrites
		server["data_path"] = gsInfo.DataPath
		server["reading_count"] = gsInfo.ReadingCount
		server["conn_counts"] = gsInfo.ConnCounts
		server["create_time"] = gsInfo.CreateTime
		server["update_time"] = gsInfo.UpdateTime
		server["group_id"] = gsInfo.GroupID

		if _, exists := groupMap[gsInfo.GroupID]; exists == true {
			group := groupMap[gsInfo.GroupID].(map[string]interface{})
			servers := group["servers"].([]interface{})
			servers = append(servers, server)
			group["servers"] = servers
		} else {
			g := make(map[string]interface{})
			g["id"] = gsInfo.GroupID
			g["group_status"] = gsInfo.GroupStatus
			g["servers"] = []interface{}{server}
			groupMap[gsInfo.GroupID] = g
		}
	}

	groups := []interface{}{}

	for _, group := range groupMap {
		groups = append(groups, group)
	}

	ctx.Resp.Header().Set("Content-Type", "application/json")
	result, _ := json.Marshal(groups)

	return http.StatusOK, result
}

func GetGroupHandler(ctx *macaron.Context, log *logrus.Logger) (int, []byte) {
	groupID := ctx.Params(":group")
	dbInstance := db.SQLDB.GetDB().(*gorm.DB)
	sqlFormat := "SELECT  * from group_server, data_server WHERE data_server.group_id=%q AND group_server.group_id=%q AND group_server.server_id = data_server.id"
	sql := fmt.Sprintf(sqlFormat, groupID, groupID)

	rows, err := dbInstance.Raw(sql).Rows()
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	defer rows.Close()

	groupMap := make(map[string]interface{})
	groupMap["id"] = groupID
	groupMap["servers"] = []interface{}{}

	for rows.Next() {
		var gsInfo models.GroupServerInfo
		dbInstance.ScanRows(rows, &gsInfo)

		server := make(map[string]interface{})
		server["data_server_id"] = gsInfo.ServerID
		server["ip"] = gsInfo.IP
		server["port"] = gsInfo.Port
		server["status"] = gsInfo.Status
		server["group_status"] = gsInfo.GroupStatus
		server["total_chunks"] = gsInfo.TotalChunks
		server["total_free_space"] = gsInfo.TotalFreeSpace
		server["max_free_space"] = gsInfo.MaxFreeSpace
		server["pending_writes"] = gsInfo.PendingWrites
		server["data_path"] = gsInfo.DataPath
		server["reading_count"] = gsInfo.ReadingCount
		server["conn_counts"] = gsInfo.ConnCounts
		server["create_time"] = gsInfo.CreateTime
		server["update_time"] = gsInfo.UpdateTime
		server["group_id"] = gsInfo.GroupID

		servers := groupMap["servers"].([]interface{})
		servers = append(servers, server)
		groupMap["servers"] = servers
	}

	numServers := len(groupMap["servers"].([]interface{}))
	if numServers == 0 {
		return http.StatusNotFound, []byte("Not Found(Group not found)")
	}

	result, _ := json.Marshal(groupMap)

	ctx.Resp.Header().Set("Content-Type", "application/json")
	return http.StatusOK, result
}
