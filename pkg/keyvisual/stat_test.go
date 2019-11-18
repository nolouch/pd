// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package keyvisual

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
)

func newRegionInfo(start string, end string, writtenBytes uint64, writtenKeys uint64, readBytes uint64, readKeys uint64) *core.RegionInfo {
	meta := &metapb.Region{}
	return core.NewRegionInfo(meta, nil,
		core.WithStartKey([]byte(start)),
		core.WithEndKey([]byte(end)),
		core.SetWrittenBytes(writtenBytes),
		core.SetWrittenKeys(writtenKeys),
		core.SetReadBytes(readBytes),
		core.SetReadKeys(readKeys),
	)
}

var debugLayersConfig = LayersConfig{
	{Len: 15, Ratio: 5},
	{Len: 60 / 5, Ratio: 12},
	{Len: 0, Ratio: 0},
}

func TestStat(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStats{})

type testStats struct{}

func (t *testStats) TestStatAppend(c *C) {
	testStat := NewStat(debugLayersConfig)
	check := func(src *matrix.DiscreteAxis, dst *matrix.DiscreteAxis) {
		srcValue := make([]*statUnit, 0)
		dstValue := make([]*statUnit, 0)
		for _, value := range src.Lines {
			srcValue = append(srcValue, value.Value.(*statUnit))
		}
		for _, value := range dst.Lines {
			dstValue = append(dstValue, value.Value.(*statUnit))
		}
		c.Assert(src.StartKey, Equals, dst.StartKey)
		for i, j := 0, 0; i < len(srcValue) && j < len(dstValue); i++ {
			c.Assert(srcValue[i].Max.ReadKeys, Equals, dstValue[j].Max.ReadKeys)
			c.Assert(srcValue[i].Max.ReadBytes, Equals, dstValue[j].Max.ReadBytes)
			c.Assert(srcValue[i].Max.WrittenKeys, Equals, dstValue[j].Max.WrittenKeys)
			c.Assert(srcValue[i].Max.WrittenBytes, Equals, dstValue[j].Max.WrittenBytes)
			j++
		}
	}

	// insert to layer 0.
	regions := [][]*core.RegionInfo{
		{
			newRegionInfo("", GenTablePrefix(2), 10, 20, 10, 20),
			newRegionInfo(GenTablePrefix(2), GenTablePrefix(3), 15, 20, 15, 30),
			newRegionInfo(GenTablePrefix(3), GenTablePrefix(5), 30, 30, 40, 40),
			newRegionInfo(GenTablePrefix(5), GenTablePrefix(7), 50, 50, 50, 50),
		},
		{
			newRegionInfo("", GenTablePrefix(2), 15, 40, 30, 50),
			newRegionInfo(GenTablePrefix(2), GenTablePrefix(3), 25, 50, 65, 100),
			newRegionInfo(GenTablePrefix(3), GenTablePrefix(5), 130, 130, 140, 140),
			newRegionInfo(GenTablePrefix(5), GenTablePrefix(7), 500, 200, 550, 550),
		},
		{
			newRegionInfo("", GenTablePrefix(2), 30, 40, 10, 20),
			newRegionInfo(GenTablePrefix(2), GenTablePrefix(3), 105, 200, 105, 300),
			newRegionInfo(GenTablePrefix(3), GenTablePrefix(5), 130, 130, 140, 140),
			newRegionInfo(GenTablePrefix(5), GenTablePrefix(7), 150, 150, 150, 150),
		},
	}
	for i := 0; i < len(regions); i++ {
		testStat.Append(regions[i])
	}
	for i, axis := range testStat.layers[0].ring {
		if i == len(regions) {
			break
		}
		check(axis, newDiscreteAxis(regions[i]))
	}

	// insert 15 more
	for j := 0; j < 5; j++ {
		for i := 0; i < len(regions); i++ {
			testStat.Append(regions[i])
		}
	}
	compactValues := make([]*statUnit, 0)
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{30, 30, 40, 50},
		Average: regionValue{55, 90, 160, 160},
	})
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{105, 105, 200, 300},
		Average: regionValue{185, 265, 340, 560},
	})
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{130, 140, 130, 140},
		Average: regionValue{450, 500, 450, 500},
	})
	compactValues = append(compactValues, &statUnit{
		Max:     regionValue{500, 550, 200, 550},
		Average: regionValue{1250, 1350, 650, 1350},
	})
	compactLines := make([]*matrix.Line, 0)
	compactLines = append(compactLines, &matrix.Line{
		EndKey: GenTablePrefix(2),
		Value:  compactValues[0],
	})
	compactLines = append(compactLines, &matrix.Line{
		EndKey: GenTablePrefix(3),
		Value:  compactValues[1],
	})
	compactLines = append(compactLines, &matrix.Line{
		EndKey: GenTablePrefix(5),
		Value:  compactValues[2],
	})
	compactLines = append(compactLines, &matrix.Line{
		EndKey: GenTablePrefix(7),
		Value:  compactValues[3],
	})
	compactDiscreteAxies := &matrix.DiscreteAxis{
		StartKey: "",
		Lines:    compactLines,
		EndTime:  time.Now(),
	}
	check(testStat.layers[1].ring[0], compactDiscreteAxies)
	head := testStat.layers[0].head
	tail := testStat.layers[0].tail
	length := testStat.layers[0].len
	for i := head; i != tail; i = (i + 1) % length {
		check(testStat.layers[0].ring[i], newDiscreteAxis(regions[i%len(regions)]))
	}
}

func check(c *C, src *statUnit, dst *statUnit) {
	c.Assert(src.Average.WrittenBytes, Equals, dst.Average.WrittenBytes)
	c.Assert(src.Average.WrittenKeys, Equals, dst.Average.WrittenKeys)
	c.Assert(src.Average.ReadBytes, Equals, dst.Average.ReadBytes)
	c.Assert(src.Average.ReadKeys, Equals, dst.Average.ReadKeys)

	c.Assert(src.Max.WrittenBytes, Equals, dst.Max.WrittenBytes)
	c.Assert(src.Max.WrittenKeys, Equals, dst.Max.WrittenKeys)
	c.Assert(src.Max.ReadBytes, Equals, dst.Max.ReadBytes)
	c.Assert(src.Max.ReadKeys, Equals, dst.Max.ReadKeys)

}

func (s testStats) TestStatUnitSplit(c *C) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Split(2)
	check(c, dst.(*statUnit), &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			50, 100, 150, 200,
		},
	})
	dst = src.Split(5)
	check(c, dst.(*statUnit), &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			20, 40, 60, 80,
		},
	})
}

func (s *testStats) TestStatUnitMerge(c *C) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			20, 40, 60, 80,
		},
	}
	src.Merge(dst)
	check(c, src, &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			120, 240, 360, 480,
		},
	})
}

func (s *testStats) TestStatUnitLess(c *C) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	src2 := &statUnit{
		Max: regionValue{
			70, 80, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	threshold := uint64(30)
	check := func(src bool, dst bool) {
		c.Assert(src, Equals, dst)
	}
	check(src.Less(threshold, "write_key"), true)
	check(src2.Less(threshold, "write_key"), false)
}

func (s *testStats) TestStatUnitGetThreshold(c *C) {
	src := []*statUnit{
		{
			Max: regionValue{
				10, 20, 30, 40,
			},
			Average: regionValue{
				100, 200, 300, 400,
			},
		},
		{
			Max: regionValue{
				70, 80, 30, 40,
			},
			Average: regionValue{
				100, 200, 300, 400,
			},
		},
		{
			Max: regionValue{
				50, 45, 40, 70,
			},
			Average: regionValue{
				100, 200, 300, 400,
			},
		},
	}
	var threshold uint64
	for _, s := range src {
		threshold = s.GetValue("")
	}
	c.Assert(threshold, Equals, 50)
}

func (s *testStats) TestStatUnitClone(c *C) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Clone()
	check(c, src, dst.(*statUnit))
}

func (s *testStats) TestStatUnitReset(c *C) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	src.Reset()
	check(c, src, &statUnit{})
}

func (s *testStats) TestStatUnitDefault(c *C) {
	src := &statUnit{
		Max: regionValue{
			10, 20, 30, 40,
		},
		Average: regionValue{
			100, 200, 300, 400,
		},
	}
	dst := src.Default()
	check(c, dst.(*statUnit), &statUnit{})
}

func (s *testStats) TestLayerStatSearch(c *C) {
	endTime := time.Now()
	l := layerStat{
		startTime:     time.Now(),
		ring:          make([]*matrix.DiscreteAxis, 0),
		head:          0,
		tail:          0,
		empty:         false,
		len:           15,
		compactRatio:  0,
		nextLayerStat: nil,
	}

	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-10 * time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-5 * time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-2 * time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime.Add(-time.Minute),
	})
	l.Append(&matrix.DiscreteAxis{
		StartKey: "",
		Lines:    nil,
		EndTime:  endTime,
	})

	find, _ := l.Search(endTime.Add(-7 * time.Minute))
	c.Assert(1, Equals, find)
	find, _ = l.Search(endTime.Add(-2 * time.Minute))
	c.Assert(2, Equals, find)
	find, _ = l.Search(endTime.Add(-2 * time.Minute))
	c.Assert(2, Equals, find)
}

func (s *testStats) TestAppend(c *C) {
	testStat := NewStat(debugLayersConfig)
	// Open our jsonFile
	jsonFile, err := os.Open("./data_test/region1.txt")
	// if we os.Open returns an error then handle it
	c.Assert(err, IsNil)
	defer jsonFile.Close()
	byteValue, err := ioutil.ReadAll(jsonFile)
	c.Assert(err, IsNil)
	res := make([]api.RegionInfo, 0, 1024)
	json.Unmarshal(byteValue, &res)
	// testStat := NewStat(debugLayersConfig)
	// testStat.Append(regions)
	//fmt.Printf("%#v\n", res[100])
	regions := make([]*core.RegionInfo, len(res))
	for i, r := range res {
		regions[i] = toCoreRegion(r)
	}
	//axis := newDiscreteAxis(regions)
	//axis.DeNoise(1)
	//d, _ := json.Marshal(axis)
	//fmt.Println(string(d))
	//fmt.Println(len(axis.Lines), len(regions))
	testStat.Append(regions)
	testStat.Append(regions)
	et := time.Now()
	st := et.Add(-60 * time.Minute)
	matrix := testStat.RangeMatrix(st, et, "", "", "write_bytes")
	d, _ := json.Marshal(matrix)
	fmt.Println(string(d), len(matrix.Data), len(matrix.Data[0]))
}

func (s *testStats) TestJson(c *C) {
	data := `
        {
        "id": 2,
        "start_key": "7480000000000000FF3C5F728000000000FF10EA720000000000FA",
        "end_key": "",
        "epoch": {
            "conf_ver": 104,
            "version": 3027
        },
        "peers": [
            {
                "id": 42988,
                "store_id": 1
            },
            {
                "id": 44771,
                "store_id": 5
            },
            {
                "id": 44835,
                "store_id": 4
            }
        ],
        "leader": {
            "id": 42988,
            "store_id": 1
        },
        "approximate_size": 56,
        "approximate_keys": 243478
    }`
	var region api.RegionInfo
	json.Unmarshal([]byte(data), &region)
	fmt.Println(toCoreRegion(region))
}

func toCoreRegion(aRegion api.RegionInfo) *core.RegionInfo {
	startKey, _ := hex.DecodeString(aRegion.StartKey)
	endKey, _ := hex.DecodeString(aRegion.EndKey)
	meta := &metapb.Region{
		Id:          aRegion.ID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: aRegion.RegionEpoch,
		Peers:       aRegion.Peers,
	}
	return core.NewRegionInfo(meta, aRegion.Leader,
		core.SetApproximateKeys(aRegion.ApproximateKeys),
		core.SetApproximateSize(aRegion.ApproximateSize),
		core.WithPendingPeers(aRegion.PendingPeers),
		core.WithDownPeers(aRegion.DownPeers),
		core.SetWrittenBytes(aRegion.WrittenBytes),
		core.SetWrittenKeys(aRegion.WrittenKeys),
		core.SetReadBytes(aRegion.ReadBytes),
		core.SetReadKeys(aRegion.ReadKeys),
	)
}
