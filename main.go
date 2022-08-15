package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SSSOC-CAN/fluke-laniakea-plugin/cfg"
	sdk "github.com/SSSOC-CAN/laniakea-plugin-sdk"
	"github.com/SSSOC-CAN/laniakea-plugin-sdk/proto"
	bg "github.com/SSSOCPaulCote/blunderguard"
	"github.com/hashicorp/go-plugin"
	"github.com/konimarti/opc"
)

type Tag struct {
	name string
	tag  string
}

var (
	pluginName                                    = "fluke-plugin"
	pluginVersion                                 = "1.0.0"
	laniVersionConstraint                         = ">= 0.2.0"
	TelemetryDefaultPollingInterval int64         = 10
	MinTelemetryPollingInterval     int64         = 5
	TelemetryPressureChannel        int64         = 81
	flukeOPCServerName                            = "Fluke.DAQ.OPC"
	flukeOPCServerHost                            = "localhost"
	defaultPolInterval              time.Duration = 5 * time.Second
	ErrAlreadyRecording                           = bg.Error("already recording")
)

type DAQConnection struct {
	opc.Connection
	Tags   []string
	TagMap map[int]Tag
}

// GetAllTags returns a slice of all detected tags
func GetAllTags() ([]string, error) {
	b, err := opc.CreateBrowser(
		flukeOPCServerName,
		[]string{flukeOPCServerHost},
	)
	if err != nil {
		return []string{}, err
	}
	return opc.CollectTags(b), nil
}

// createTagMap takes the tag map given in the config file and creates a proper tag map from it
func createTagMap(tags []string, cfgTagMap map[int]string) map[int]Tag {
	tagMap := make(map[int]Tag)
	for i, str := range cfgTagMap {
		tagMap[i] = Tag{name: str, tag: tags[i]}
	}
	return tagMap
}

// ConnectToDAQ establishes a connection with the OPC server of the Fluke DAQ software and the FMTD
func ConnectToDAQ(cfgTags map[int]string) (*DAQConnection, error) {
	tags, err := GetAllTags()
	if err != nil {
		return nil, err
	}
	c, err := opc.NewConnection(
		flukeOPCServerName,
		[]string{flukeOPCServerHost},
		tags,
	)
	if err != nil {
		return nil, err
	}
	return &DAQConnection{
		Connection: c,
		Tags:       tags,
		TagMap:     createTagMap(tags, cfgTags),
	}, nil
}

// StartScanning starts the scanning process on the DAQ
func (d *DAQConnection) StartScanning() error {
	err := d.Write(d.TagMap[0].tag, true)
	if err != nil {
		return err
	}
	return nil
}

// StopScanning stops the scanning process on the DAQ
func (d *DAQConnection) StopScanning() error {
	err := d.Write(d.TagMap[0].tag, false)
	if err != nil {
		return err
	}
	return nil
}

// GetTagMapNames returns a slice of all the TagMap names
func (d *DAQConnection) GetTagMapNames() []string {
	idxs := make([]int, 0, len(d.TagMap))
	for idx := range d.TagMap {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	names := make([]string, 0, len(idxs)-1)
	for _, i := range idxs {
		if i != 0 {
			names = append(names, d.TagMap[i].name)
		}
	}
	return names
}

type Reading struct {
	Item opc.Item
	Name string
}

// ReadItems returns a slice of all readings
func (d *DAQConnection) ReadItems() []Reading {
	idxs := make([]int, 0, len(d.TagMap))
	for idx := range d.TagMap {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	readings := make([]Reading, 0, len(idxs)-1)
	for _, i := range idxs {
		if i != 0 {
			readings = append(readings, Reading{
				Item: d.ReadItem(d.TagMap[i].tag),
				Name: d.TagMap[i].name,
			})
		}
	}
	return readings
}

type FlukeDatasource struct {
	sdk.DatasourceBase
	recording  int32 // used atomically
	quitChan   chan struct{}
	connection *DAQConnection
	config     *cfg.Config
	sync.WaitGroup
}

type Payload struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

type Frame struct {
	Data []Payload `json:"data"`
}

// Compile time check to ensure DemoDatasource satisfies the Datasource interface
var _ sdk.Datasource = (*FlukeDatasource)(nil)

// Implements the Datasource interface funciton StartRecord
func (e *FlukeDatasource) StartRecord() (chan *proto.Frame, error) {
	if atomic.LoadInt32(&e.recording) == 1 {
		return nil, ErrAlreadyRecording
	}
	// start connection
	err := e.connection.StartScanning()
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(defaultPolInterval)
	frameChan := make(chan *proto.Frame)
	if ok := atomic.CompareAndSwapInt32(&e.recording, 0, 1); !ok {
		return nil, ErrAlreadyRecording
	}
	e.Add(1)
	go func() {
		defer e.Done()
		defer close(frameChan)
		time.Sleep(1 * time.Second) // sleep for a second while laniakea sets up the plugin
		for {
			select {
			case <-ticker.C:
				data := []Payload{}
				df := Frame{}
				readings := e.connection.ReadItems()
				for _, reading := range readings {
					switch v := reading.Item.Value.(type) {
					case float64:
						data = append(data, Payload{Name: reading.Name, Value: v})
					case float32:
						data = append(data, Payload{Name: reading.Name, Value: float64(v)})
					}
				}
				df.Data = data[:]
				// transform to json string
				b, err := json.Marshal(&df)
				if err != nil {
					log.Println(err)
					return
				}
				frameChan <- &proto.Frame{
					Source:    pluginName,
					Type:      "application/json",
					Timestamp: time.Now().UnixMilli(),
					Payload:   b,
				}
			case <-e.quitChan:
				ticker.Stop()
				err := e.connection.StopScanning()
				if err != nil {
					log.Println(err)
				}
				return
			}
		}
	}()
	return frameChan, nil
}

// Implements the Datasource interface funciton StopRecord
func (e *FlukeDatasource) StopRecord() error {
	e.quitChan <- struct{}{}
	return nil
}

// Implements the Datasource interface funciton Stop
func (e *FlukeDatasource) Stop() error {
	close(e.quitChan)
	e.Wait()
	return nil
}

func main() {
	config, err := cfg.InitConfig()
	if err != nil {
		log.Println(err)
		return
	}
	conn, err := ConnectToDAQ(config.FlukeTags)
	if err != nil {
		log.Println(err)
		return
	}
	impl := &FlukeDatasource{quitChan: make(chan struct{}), connection: conn, config: config}
	impl.SetPluginVersion(pluginVersion)              // set the plugin version before serving
	impl.SetVersionConstraints(laniVersionConstraint) // set required laniakea version before serving
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: sdk.HandshakeConfig,
		Plugins: map[string]plugin.Plugin{
			pluginName: &sdk.DatasourcePlugin{Impl: impl},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
