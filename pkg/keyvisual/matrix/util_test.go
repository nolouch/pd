package matrix

import (
	. "github.com/pingcap/check"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestMemset(c *C) {
	s1 := []uint64{3, 3, 3, 3, 3}
	s2 := []uint64{0, 0, 0, 0, 0}
	s3 := []uint64{6, 6, 6, 6}
	s4 := []uint64{9, 9, 9, 9}

	Memset(s1, 0)
	Memset(s3, 9)

	c.Assert(s1, DeepEquals, s2)
	c.Assert(s3, DeepEquals, s4)
}
