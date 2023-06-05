package libovsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/rpc2"
	"github.com/cenkalti/rpc2/jsonrpc"
	log "k8s.io/klog"
)

type OvsdbClient struct {
	socketPath string
	ipAddr     string
	port       int

	rpcClient *rpc2.Client
	Schema    map[string]DatabaseSchema
	handlers  []NotificationHandler

	updateMutex sync.Mutex
	ovsdbCache  map[string]map[string]Row
	monitorArgs []interface{}
	stopChan    chan struct{}
}

const DEFAULT_ADDR = "127.0.0.1"
const DEFAULT_PORT = 6640
const DEFAULT_SOCK = "/var/run/openvswitch/db.sock"

const RECONNECT_INTERVAL = 1
const RECONNECT_TIMES = 60

func newClient() *OvsdbClient {
	return &OvsdbClient{
		Schema:     make(map[string]DatabaseSchema),
		stopChan:   make(chan struct{}),
		ovsdbCache: make(map[string]map[string]Row),
	}
}

func configureClient(conn net.Conn, ovs *OvsdbClient) error {
	c := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))
	c.SetBlocking(true)
	c.Handle("echo", echo(ovs))
	c.Handle("update", update(ovs))
	ovs.rpcClient = c

	go c.Run()
	go handleDisconnectNotification(ovs)

	// Process Async Notifications
	dbs, err := ovs.ListDbs()
	if err == nil {
		for _, db := range dbs {
			schema, err := ovs.GetSchema(db)
			if err == nil {
				ovs.Schema[db] = *schema
			} else {
				return err
			}
		}
	}
	return nil
}

func connectRaw(ovs *OvsdbClient) (net.Conn, error) {
	var conn net.Conn
	var err error
	if ovs.socketPath != "" {
		conn, err = net.Dial("unix", ovs.socketPath)
		if err != nil {
			return nil, err
		}
	} else {
		target := fmt.Sprintf("%s:%d", ovs.ipAddr, ovs.port)
		conn, err = net.Dial("tcp", target)
		if err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func ConnectUnix(socketPath string) (*OvsdbClient, error) {
	if socketPath == "" {
		socketPath = DEFAULT_SOCK
	}

	ovs := newClient()
	ovs.socketPath = socketPath

	conn, err := connectRaw(ovs)
	if err != nil {
		return nil, err
	}

	if err = configureClient(conn, ovs); err != nil {
		return nil, err
	}

	return ovs, nil
}

func Connect(ipAddr string, port int) (*OvsdbClient, error) {
	if ipAddr == "" {
		ipAddr = DEFAULT_ADDR
	}

	if port <= 0 {
		port = DEFAULT_PORT
	}

	ovs := newClient()
	ovs.ipAddr = ipAddr
	ovs.port = port

	conn, err := connectRaw(ovs)
	if err != nil {
		return nil, err
	}

	if err = configureClient(conn, ovs); err != nil {
		return nil, err
	}

	return ovs, nil
}

func (ovs *OvsdbClient) Register(handler NotificationHandler) {
	ovs.handlers = append(ovs.handlers, handler)
}

type NotificationHandler interface {
	// RFC 7047 section 4.1.6 Update Notification
	Update(context interface{}, tableUpdates TableUpdates)

	// RFC 7047 section 4.1.9 Locked Notification
	Locked([]interface{})

	// RFC 7047 section 4.1.10 Stolen Notification
	Stolen([]interface{})

	// RFC 7047 section 4.1.11 Echo Notification
	Echo([]interface{})
}

// RFC 7047 : Section 4.1.6 : Echo
func echo(ovsClient *OvsdbClient) func(client *rpc2.Client, args []interface{}, reply *[]interface{}) error {
	return func(client *rpc2.Client, args []interface{}, reply *[]interface{}) error {
		*reply = args
		for _, handler := range ovsClient.handlers {
			handler.Echo(nil)
		}
		return nil
	}
}

// RFC 7047 : Update Notification Section 4.1.6
// Processing "params": [<json-value>, <table-updates>]
func update(ovsClient *OvsdbClient) func(client *rpc2.Client, params []interface{}, reply *interface{}) error {
	return func(client *rpc2.Client, params []interface{}, reply *interface{}) error {
		ovsClient.updateMutex.Lock()
		defer ovsClient.updateMutex.Unlock()

		if len(params) < 2 {
			return errors.New("invalid Update message")
		}
		// Ignore params[0] as we dont use the <json-value> currently for comparison

		raw, ok := params[1].(map[string]interface{})
		if !ok {
			return errors.New("invalid Update message")
		}
		var rowUpdates map[string]map[string]RowUpdate

		b, err := json.Marshal(raw)
		if err != nil {
			return err
		}
		err = json.Unmarshal(b, &rowUpdates)
		if err != nil {
			return err
		}

		tableUpdates := getTableUpdatesFromRawUnmarshal(rowUpdates)
		// Send updates to handlers
		for _, handler := range ovsClient.handlers {
			handler.Update(params, tableUpdates)
		}

		// Update local ovsdb cache
		ovsClient.updateCache(tableUpdates)

		return nil
	}
}

// RFC 7047 : get_schema
func (ovs *OvsdbClient) GetSchema(dbName string) (*DatabaseSchema, error) {
	args := NewGetSchemaArgs(dbName)
	var reply DatabaseSchema
	err := ovs.rpcClient.Call("get_schema", args, &reply)
	if err != nil {
		return nil, err
	} else {
		ovs.Schema[dbName] = reply
	}
	return &reply, err
}

// RFC 7047 : list_dbs
func (ovs *OvsdbClient) ListDbs() ([]string, error) {
	var dbs []string
	err := ovs.rpcClient.Call("list_dbs", nil, &dbs)
	if err != nil {
		log.Fatal("ListDbs failure", err)
	}
	return dbs, err
}

// RFC 7047 : transact

func (ovs *OvsdbClient) Transact(database string, operation ...Operation) ([]OperationResult, error) {
	var reply []OperationResult
	db, ok := ovs.Schema[database]
	if !ok {
		return nil, errors.New("invalid Database Schema")
	}

	if ok := db.validateOperations(operation...); !ok {
		return nil, errors.New("validation failed for the operation")
	}

	args := NewTransactArgs(database, operation...)
	err := ovs.rpcClient.Call("transact", args, &reply)
	if err != nil {
		log.Fatal("transact failure", err)
	}
	return reply, err
}

// Convenience method to monitor every table/column
func (ovs *OvsdbClient) MonitorAll(database string, jsonContext interface{}) error {
	schema, ok := ovs.Schema[database]
	if !ok {
		return errors.New("invalid Database Schema")
	}

	requests := make(map[string]MonitorRequest)
	for table, tableSchema := range schema.Tables {
		var columns []string
		for column := range tableSchema.Columns {
			columns = append(columns, column)
		}
		requests[table] = MonitorRequest{
			Columns: columns,
			Select: MonitorSelect{
				Initial: true,
				Insert:  true,
				Delete:  true,
				Modify:  true,
			}}
	}
	return ovs.Monitor(database, jsonContext, requests)
}

// RFC 7047 : monitor
func (ovs *OvsdbClient) Monitor(database string, jsonContext interface{}, requests map[string]MonitorRequest) error {
	ovs.updateMutex.Lock()
	defer ovs.updateMutex.Unlock()

	args := NewMonitorArgs(database, jsonContext, requests)
	ovs.monitorArgs = args

	// This totally sucks. Refer to golang JSON issue #6213
	var response map[string]map[string]RowUpdate
	if err := ovs.rpcClient.Call("monitor", args, &response); err != nil {
		return err
	}
	reply := getTableUpdatesFromRawUnmarshal(response)
	for _, handler := range ovs.handlers {
		handler.Update(nil, reply)
	}

	ovs.updateCache(reply)

	return nil
}

func (ovs *OvsdbClient) MonitorReset() error {
	ovs.updateMutex.Lock()
	defer ovs.updateMutex.Unlock()

	// skip reset if monitor not exist
	if len(ovs.monitorArgs) == 0 {
		return nil
	}

	var response map[string]map[string]RowUpdate
	if err := ovs.rpcClient.Call("monitor", ovs.monitorArgs, &response); err != nil {
		return err
	}
	reply := getTableUpdatesFromRawUnmarshal(response)

	// diff lastest db and cache for missing delete events
	var deleteUpdates TableUpdates
	deleteUpdates.Updates = make(map[string]TableUpdate)
	for table := range ovs.ovsdbCache {
		for uuid, row := range ovs.ovsdbCache[table] {
			if _, ok := reply.Updates[table].Rows[uuid]; !ok {
				if _, ok := deleteUpdates.Updates[table]; !ok {
					deleteUpdates.Updates[table] = TableUpdate{
						Rows: map[string]RowUpdate{},
					}
				}
				deleteUpdates.Updates[table].Rows[uuid] = RowUpdate{
					Uuid: UUID{GoUuid: uuid},
					Old:  row,
					New:  Row{},
				}
			}
		}
	}

	// send event to user-defined handles
	for _, handles := range ovs.handlers {
		handles.Update(nil, deleteUpdates)
		handles.Update(nil, reply)
	}

	// rebuild local db cache
	ovs.ovsdbCache = make(map[string]map[string]Row)
	ovs.updateCache(reply)

	return nil
}

func (ovs *OvsdbClient) updateCache(updates TableUpdates) {
	for table, tableUpdate := range updates.Updates {
		if _, ok := ovs.ovsdbCache[table]; !ok {
			ovs.ovsdbCache[table] = make(map[string]Row)
		}
		for uuid, row := range tableUpdate.Rows {
			empty := Row{}
			if !reflect.DeepEqual(row.New, empty) {
				ovs.ovsdbCache[table][uuid] = row.New
			} else {
				delete(ovs.ovsdbCache[table], uuid)
			}
		}
	}
}

func getTableUpdatesFromRawUnmarshal(raw map[string]map[string]RowUpdate) TableUpdates {
	var tableUpdates TableUpdates
	tableUpdates.Updates = make(map[string]TableUpdate)
	for table, update := range raw {
		tableUpdate := TableUpdate{update}
		tableUpdates.Updates[table] = tableUpdate
	}
	return tableUpdates
}

func handleDisconnectNotification(ovs *OvsdbClient) {
	for {
		select {
		case <-ovs.stopChan:
			return
		case <-ovs.rpcClient.DisconnectNotify():
			log.Errorln("ovsdb RPC client disconnect")
			for i := 0; i < RECONNECT_TIMES; i++ {
				conn, err := connectRaw(ovs)
				if err != nil {
					log.Errorf("reconnect ovsdb error, err:%s", err)
					time.Sleep(RECONNECT_INTERVAL * time.Second)
					continue
				}
				if err = configureClient(conn, ovs); err != nil {
					log.Fatalln("failed to configure ovsdb client")
				}
				if err = ovs.MonitorReset(); err != nil {
					log.Fatalln("failed to reset ovsdb monitor")
				}
				return
			}
			log.Fatalf("reconnect ovsdb failed after %d times retries", RECONNECT_TIMES)
		}
	}
}

func (ovs *OvsdbClient) Disconnect() {
	ovs.stopChan <- struct{}{}
	ovs.rpcClient.Close()
}
