package main

import (
	"context"
	"crypto/tls"
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
	influx "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/konimarti/opc"
)

type Tag struct {
	name    string
	tag     string
	tagType string
}

var (
	pluginName                               = "fluke-plugin"
	pluginVersion                            = "1.0.0"
	laniVersionConstraint                    = ">= 0.2.0"
	flukeOPCServerName                       = "Fluke.DAQ.OPC"
	flukeOPCServerHost                       = "localhost"
	defaultPolInterval         time.Duration = 5 * time.Second
	ErrAlreadyRecording                      = bg.Error("already recording")
	ErrAlreadyStoppedRecording               = bg.Error("already stopped recording")
	ErrBlankInfluxOrgOrBucket                = bg.Error("influx organization or bucket cannot be blank")
	ErrInvalidOrg                            = bg.Error("invalid influx organization")
	ErrInvalidBucket                         = bg.Error("invalid influx bucket")
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
func createTagMap(tags []string, cfgTagMap map[int]cfg.CfgTag) map[int]Tag {
	tagMap := make(map[int]Tag)
	for i, cfgTag := range cfgTagMap {
		tagMap[i] = Tag{name: cfgTag.Tag, tag: tags[i], tagType: cfgTag.Type}
	}
	return tagMap
}

// ConnectToDAQ establishes a connection with the OPC server of the Fluke DAQ software and the FMTD
func ConnectToDAQ(cfgTags map[int]cfg.CfgTag) (*DAQConnection, error) {
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
	Type string
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
				Type: d.TagMap[i].tagType,
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
	client     influx.Client
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
	var writeAPI api.WriteAPI
	if e.config.Influx {
		if e.config.InfluxOrgName == "" || e.config.InfluxBucketName == "" {
			return nil, ErrBlankInfluxOrgOrBucket
		}
		orgAPI := e.client.OrganizationsAPI()
		org, err := orgAPI.FindOrganizationByName(context.Background(), e.config.InfluxOrgName)
		if err != nil {
			return nil, ErrInvalidOrg
		}
		bucketAPI := e.client.BucketsAPI()
		buckets, err := bucketAPI.FindBucketsByOrgName(context.Background(), e.config.InfluxOrgName)
		if err != nil {
			return nil, ErrInvalidOrg
		}
		var found bool
		for _, bucket := range *buckets {
			if bucket.Name == e.config.InfluxBucketName {
				found = true
				break
			}
		}
		if !found {
			log.Printf("Creating %s bucket...", e.config.InfluxBucketName)
			_, err := bucketAPI.CreateBucketWithName(context.Background(), org, e.config.InfluxBucketName, domain.RetentionRule{EverySeconds: 0})
			if err != nil {
				return nil, err
			}
		}
		writeAPI = e.client.WriteAPI(e.config.InfluxOrgName, e.config.InfluxBucketName)
	}
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
				current_time := time.Now()
				for _, reading := range readings {
					switch v := reading.Item.Value.(type) {
					case float64:
						data = append(data, Payload{Name: reading.Name, Value: v})
						if e.config.Influx {
							if reading.Type != "ignore" {
								p := influx.NewPoint(
									reading.Type,
									map[string]string{
										"id": reading.Name,
									},
									map[string]interface{}{
										reading.Type: v,
									},
									current_time,
								)
								// write asynchronously
								writer.WritePoint(p)
							}
						}
					case float32:
						data = append(data, Payload{Name: reading.Name, Value: v})
						if e.config.Influx {
							if reading.Type != "ignore" {
								p := influx.NewPoint(
									reading.Type,
									map[string]string{
										"id": reading.Name,
									},
									map[string]interface{}{
										reading.Type: float64(v),
									},
									current_time,
								)
								// write asynchronously
								writer.WritePoint(p)
							}
						}
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
					Timestamp: current_time.UnixMilli(),
					Payload:   b,
				}
			case <-e.quitChan:
				ticker.Stop()
				err := e.connection.StopScanning()
				if err != nil {
					log.Println(err)
				}
				if e.config.Influx {
					writeAPI.Flush()
					e.client.Close()
				}
				return
			}
		}
	}()
	return frameChan, nil
}

// Implements the Datasource interface funciton StopRecord
func (e *FlukeDatasource) StopRecord() error {
	if ok := atomic.CompareAndSwapInt32(&e.recording, 1, 0); !ok {
		return ErrAlreadyStoppedRecording
	}
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
	if config.Influx {
		if config.InfluxURL == "" || config.InfuxAPIToken == "" {
			log.Println("Influx URL or API Token config parameters cannot be blank")
		}
		impl.client = influx.NewClientWithOptions(config.InfluxURL, config.InfuxAPIToken, influx.DefaultOptions().SetTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	}
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
