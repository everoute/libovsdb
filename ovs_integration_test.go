package libovsdb

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestConnectUnix(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	f, err := os.Open(DEFAULT_SOCK)
	if err != nil {
		t.Skip("Missing OVSDB unix socket")
	}
	f.Close()

	timeoutChan := make(chan bool)
	connected := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- true
	}()

	go func() {
		ovs, err := ConnectUnix("")
		if err != nil {
			connected <- false
		} else {
			connected <- true
			ovs.Disconnect()
		}
	}()

	select {
	case <-timeoutChan:
		t.Error("Connection Timed Out")
	case b := <-connected:
		if !b {
			t.Error("Couldnt connect to OVSDB Server")
		}
	}
}

func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c, err := net.Dial("tcp", os.Getenv("DOCKER_IP")+":6640")
	if err != nil {
		t.Skip("No OVSDB connection over TCP")
	}
	c.Close()

	timeoutChan := make(chan bool)
	connected := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- true
	}()

	go func() {
		// Use Convenience params. Ignore failure even if any
		_, err := Connect("", 0)
		if err != nil {
			log.Println("Couldnt establish OVSDB connection with Default params. No big deal")
		}
	}()

	go func() {
		ovs, err := Connect(os.Getenv("DOCKER_IP"), int(6640))
		if err != nil {
			connected <- false
		} else {
			connected <- true
			ovs.Disconnect()
		}
	}()

	select {
	case <-timeoutChan:
		t.Error("Connection Timed Out")
	case b := <-connected:
		if !b {
			t.Error("Couldnt connect to OVSDB Server")
		}
	}
}

func getOvsClient() (*OvsdbClient, error) {
	ovs, err := Connect(os.Getenv("DOCKER_IP"), int(6640))
	if err != nil {
		ovs, err = ConnectUnix("")
		if err != nil {
			panic(err)
		}
	}

	return ovs, err
}

func TestListDbs(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ovs, err := getOvsClient()
	reply, err := ovs.ListDbs()

	if err != nil {
		log.Fatal("ListDbs error:", err)
	}

	if reply[0] != "Open_vSwitch" {
		t.Error("Expected: 'Open_vSwitch', Got:", reply)
	}
	var b bytes.Buffer
	ovs.Schema[reply[0]].Print(&b)
	ovs.Disconnect()
}

func TestGetSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ovs, err := getOvsClient()
	if err != nil {
		panic(err)
	}

	var dbName string = "Open_vSwitch"
	reply, err := ovs.GetSchema(dbName)

	if err != nil {
		log.Fatal("GetSchemas error:", err)
		t.Error("Error Processing GetSchema for ", dbName, err)
	}

	if reply.Name != dbName {
		t.Error("Schema Name mismatch. Expected: ", dbName, "Got: ", reply.Name)
	}
	ovs.Disconnect()
}

var bridgeName string = "gopher-br7"
var bridgeNameDisconnect string = "br-disconnect"
var bridgeUuid string

func TestInsertTransact(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, err := getOvsClient()
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}

	// NamedUuid is used to add multiple related Operations in a single Transact operation
	namedUuid := "gopher"

	externalIds := make(map[string]string)
	externalIds["go"] = "awesome"
	externalIds["docker"] = "made-for-each-other"
	oMap, err := NewOvsMap(externalIds)
	// bridge row to insert
	bridge := make(map[string]interface{})
	bridge["name"] = bridgeName
	bridge["external_ids"] = oMap

	// simple insert operation
	insertOp := Operation{
		Op:       "insert",
		Table:    "Bridge",
		Row:      bridge,
		UUIDName: namedUuid,
	}

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	mutateUuid := []UUID{UUID{namedUuid}}
	mutateSet, _ := NewOvsSet(mutateUuid)
	mutation := NewMutation("bridges", "insert", mutateSet)
	// hacked Condition till we get Monitor / Select working
	condition := NewCondition("_uuid", "!=", UUID{"2f77b348-9768-4866-b761-89d5177ecdab"})

	// simple mutate operation
	mutateOp := Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []Operation{insertOp, mutateOp}
	reply, err := ovs.Transact("Open_vSwitch", operations...)

	if len(reply) < len(operations) {
		t.Error("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			t.Error("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			t.Error("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUuid)
		bridgeUuid = reply[0].UUID.GoUuid
	}
	ovs.Disconnect()
}

func TestDeleteTransact(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	if bridgeUuid == "" {
		t.Skip()
	}

	ovs, err := getOvsClient()
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}

	// simple delete operation
	condition := NewCondition("name", "==", bridgeName)
	deleteOp := Operation{
		Op:    "delete",
		Table: "Bridge",
		Where: []interface{}{condition},
	}

	// Deleting a Bridge row in Bridge table requires mutating the open_vswitch table.
	mutateUuid := []UUID{UUID{bridgeUuid}}
	mutateSet, _ := NewOvsSet(mutateUuid)
	mutation := NewMutation("bridges", "delete", mutateSet)
	// hacked Condition till we get Monitor / Select working
	condition = NewCondition("_uuid", "!=", UUID{"2f77b348-9768-4866-b761-89d5177ecdab"})

	// simple mutate operation
	mutateOp := Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []Operation{deleteOp, mutateOp}
	reply, err := ovs.Transact("Open_vSwitch", operations...)

	if len(reply) < len(operations) {
		t.Error("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			t.Error("Transaction Failed due to an error :", o.Error, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			t.Error("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Delete Successful", reply[0].Count)
	}
	ovs.Disconnect()
}

func TestMonitor(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, err := getOvsClient()
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}

	err = ovs.MonitorAll("Open_vSwitch", nil)

	if err != nil {
		t.Error("Monitor operation failed with error=", err)
	}
	ovs.Disconnect()
}

func TestDisconnect(t *testing.T) {

	ovs, err := getOvsClient()
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}
	updatesChan := make(chan TableUpdates, 10)
	ovs.Register(DisconnectNotify{updatesChan})

	err = ovs.MonitorAll("Open_vSwitch", nil)
	if err != nil {
		t.Error("Monitor operation failed with error=", err)
	}

	out, err := exec.Command("/bin/sh", "-c", "/usr/share/openvswitch/scripts/ovs-ctl restart").CombinedOutput()
	if err != nil {
		t.Errorf("fail to restart ovsdb, out: %s err: %s\n", out, err)
	}

	out, err = exec.Command("/bin/sh", "-c", "ovs-vsctl add-br "+bridgeNameDisconnect).CombinedOutput()
	if err != nil {
		t.Errorf("fail to create bridge, out: %s err: %s\n", out, err)
	}

loopTest:
	for {
		select {
		case newUpdates := <-updatesChan:
			for table := range newUpdates.Updates {
				if table == "Bridge" {
					for _, raw := range newUpdates.Updates[table].Rows {
						if v, ok := raw.New.Fields["name"]; ok {
							if brName, ok := v.(string); ok {
								if brName == bridgeNameDisconnect {
									break loopTest
								}
							}
						}
					}
				}
			}
		case <-time.After(time.Second * 10):
			t.Error("No Brige update message in 10 seconds")
			return
		}

	}

	ovs.Disconnect()
}

type DisconnectNotify struct {
	updates chan TableUpdates
}

func (n DisconnectNotify) Update(context interface{}, tableUpdates TableUpdates) {
	n.updates <- tableUpdates
}
func (n DisconnectNotify) Locked([]interface{}) {
}
func (n DisconnectNotify) Stolen([]interface{}) {
}
func (n DisconnectNotify) Echo([]interface{}) {
}

func TestNotify(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ovs, err := getOvsClient()
	if err != nil {
		log.Fatal("Failed to Connect. error:", err)
		panic(err)
	}

	notifyEchoChan := make(chan bool, 1)

	notifier := Notifier{notifyEchoChan}
	ovs.Register(notifier)
	_ = ovs.MonitorAll("Open_vSwitch", nil)

	if _, err = exec.Command("/bin/sh", "-c", "ovs-vsctl add-br test-br").CombinedOutput(); err != nil {
		t.Error("Failed to breate bridge")
	}

	timeoutChan := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- true
	}()

	select {
	case <-timeoutChan:
		t.Error("No Echo message notify in 10 seconds")
	case <-notifyEchoChan:
		break
	}
	ovs.Disconnect()
}

type Notifier struct {
	notifyChan chan bool
}

func (n Notifier) Update(context interface{}, tableUpdates TableUpdates) {
	n.notifyChan <- true
}
func (n Notifier) Locked([]interface{}) {
}
func (n Notifier) Stolen([]interface{}) {
}
func (n Notifier) Echo([]interface{}) {
	n.notifyChan <- true
}

func TestDBSchemaValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := getOvsClient()
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	_, err := ovs.Transact("Invalid_DB", operation)
	if err == nil {
		t.Error("Invalid DB operation Validation failed")
	}

	ovs.Disconnect()
}

func TestTableSchemaValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := getOvsClient()
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := Operation{
		Op:    "insert",
		Table: "InvalidTable",
		Row:   bridge,
	}
	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Table Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInRowValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := getOvsClient()
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"
	bridge["invalid_column"] = "invalid_column"

	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInMultipleRowsValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := getOvsClient()
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	rows := make([]map[string]interface{}, 2)

	invalidBridge := make(map[string]interface{})
	invalidBridge["invalid_column"] = "invalid_column"

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	rows[0] = invalidBridge
	rows[1] = bridge
	operation := Operation{
		Op:    "insert",
		Table: "Bridge",
		Rows:  rows,
	}
	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaValidation(t *testing.T) {

	if testing.Short() {
		t.Skip()
	}

	ovs, e := getOvsClient()
	if e != nil {
		log.Fatal("Failed to Connect. error:", e)
		panic(e)
	}

	operation := Operation{
		Op:      "select",
		Table:   "Bridge",
		Columns: []string{"name", "invalidColumn"},
	}
	_, err := ovs.Transact("Open_vSwitch", operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}
