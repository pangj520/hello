package servicediscovery

import (
	"context"
	"fmt"
	"github.com/edwingeng/doublejump"
	"github.com/tatsushid/go-fastping"
	"github.com/valyala/fastrand"
	"hash/fnv"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

//路由选择器
//	支持随机、轮询、权重、网络质量、一致性哈希、自定义等路由选择方式，默认为随机选择

//路由选择方式
type SelectMode string

//可选选择方式
const (
	RandomSelect       SelectMode = "Random"             //随机
	RoundRobin                    = "RoundRobin"         //轮询
	WeightedRoundRobin            = "WeightedRoundRobin" //权重
	WeightedICMP                  = "WeightedICMP"       //网络质量优先，通过ping测试
	ConsistentHash                = "ConsistentHash"     //一致性哈希
	Custom                        = "Custom"             //自定义
)

type SelectFunc func(ctx context.Context) Service

// Selector defines selector that selects one service from candidates.
type Selector interface {
	Select(ctx context.Context) Service // SelectFunc
	//UpdateServer(services []Service) //直接覆盖原有的
}

//创建选择器
func NewSelector(selectMode SelectMode, services []Service) Selector {
	switch selectMode {
	case RandomSelect:
		return newRandomSelector(services)
	case RoundRobin:
		return newRoundRobinSelector(services)
	case WeightedRoundRobin:
		return newWeightedRoundRobinSelector(services)
	case WeightedICMP:
		return newWeightedICMPSelector(services)
	case ConsistentHash:
		return newConsistentHashSelector(services)
	case Custom:
		return nil
	default:
		return newRandomSelector(services)
	}
}

// 随机选择器.
type randomSelector struct {
	mu sync.Mutex
	services []Service
}

func newRandomSelector(services []Service) Selector {
	ss := make([]Service, 0, len(services))
	for _, s := range services {
		ss = append(ss, s)
	}

	return &randomSelector{services: ss, mu: sync.Mutex{}}
}

func (s randomSelector) Select(ctx context.Context) Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.services
	if len(ss) == 0 {
		return Service{}
	}
	i := fastrand.Uint32n(uint32(len(ss)))
	return ss[i]
}

func (s *randomSelector) UpdateServer(services []Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := make([]Service, 0, len(services))
	for _, s := range services {
		ss = append(ss, s)
	}

	s.services = ss
}

//轮询选择器.
type roundRobinSelector struct {
	mu sync.Mutex
	services []Service
	i        int
}

func newRoundRobinSelector(services []Service) Selector {
	ss := make([]Service, 0, len(services))
	for _, s := range services {
		ss = append(ss, s)
	}

	return &roundRobinSelector{services: ss, mu: sync.Mutex{}}
}

func (s *roundRobinSelector) Select(ctx context.Context) Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.services
	if len(ss) == 0 {
		return Service{}
	}
	i := s.i
	i = i % len(ss)
	s.i = i + 1

	return ss[i]
}

func (s *roundRobinSelector) UpdateServer(services []Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := make([]Service, 0, len(services))
	for _, s := range services {
		ss = append(ss, s)
	}

	s.services = ss
}

// Weighted is a wrapped server with  weight
type Weighted struct {
	Server          Service
	Weight          int
	CurrentWeight   int
	EffectiveWeight int
}

// func (w *Weighted) fail() {
// 	w.EffectiveWeight -= w.Weight
// 	if w.EffectiveWeight < 0 {
// 		w.EffectiveWeight = 0
// 	}
// }

//https://github.com/phusion/nginx/commit/27e94984486058d73157038f7950a0a36ecc6e35
func nextWeighted(servers []*Weighted) (best *Weighted) {
	total := 0

	for i := 0; i < len(servers); i++ {
		w := servers[i]

		if w == nil {
			continue
		}
		//if w is down, continue

		w.CurrentWeight += w.EffectiveWeight
		total += w.EffectiveWeight

		if best == nil || w.CurrentWeight > best.CurrentWeight {
			best = w
		}

	}

	if best == nil {
		return nil
	}

	best.CurrentWeight -= total
	return best
}

// 权重选择器.
type weightedRoundRobinSelector struct {
	mu sync.Mutex
	services []*Weighted
}

func newWeightedRoundRobinSelector(services []Service) Selector {
	ss := createWeighted(services)
	return &weightedRoundRobinSelector{services: ss, mu: sync.Mutex{}}
}

func (s *weightedRoundRobinSelector) Select(ctx context.Context) Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.services
	if len(ss) == 0 {
		return Service{}
	}
	w := nextWeighted(ss)
	if w == nil {
		return Service{}
	}
	return w.Server
}

func (s *weightedRoundRobinSelector) UpdateServer(services []Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := createWeighted(services)
	s.services = ss
}

func createWeighted(services []Service) []*Weighted {
	ss := make([]*Weighted, 0, len(services))
	for _, s := range services {
		w := &Weighted{Server: s, Weight: 1, EffectiveWeight: 1}
		ss = append(ss, w)
		if s.Meta == nil {
			continue
		}
		ww, ok := s.Meta["Weight"]
		if !ok {
			continue
		}
		if ww == "" {
			continue
		}
		if weight, err := strconv.Atoi(ww); err == nil {
			w.Weight = weight
			w.EffectiveWeight = weight
		}
	}

	return ss
}

type geoServer struct {
	Server    Service
	Latitude  float64
	Longitude float64
}

//地理位置选择器.
type geoSelector struct {
	mu sync.Mutex
	services  []*geoServer
	Latitude  float64
	Longitude float64
	r         *rand.Rand
}

func newGeoSelector(services []Service, latitude, longitude float64) Selector {
	ss := createGeoServer(services)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &geoSelector{services: ss, Latitude: latitude, Longitude: longitude, r: r, mu: sync.Mutex{}}
}

func (s geoSelector) Select(ctx context.Context) Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.services) == 0 {
		return Service{}
	}

	var server []Service
	min := math.MaxFloat64
	for _, gs := range s.services {
		d := getDistanceFrom(s.Latitude, s.Longitude, gs.Latitude, gs.Longitude)
		if d < min {
			server = []Service{gs.Server}
			min = d
		} else if d == min {
			server = append(server, gs.Server)
		}
	}
	if len(server) == 0 {
		return Service{}
	}

	if len(server) == 1 {
		return server[0]
	}

	return server[s.r.Intn(len(server))]
}

func (s *geoSelector) UpdateServer(services []Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := createGeoServer(services)
	s.services = ss
}

func createGeoServer(services []Service) []*geoServer {
	geoServers := make([]*geoServer, 0, len(services))

	for _, s := range services {
		if s.Meta == nil {
			continue
		}
		latStr := s.Meta["Latitude"]
		lonStr := s.Meta["Longitude"]

		if latStr == "" || lonStr == "" {
			continue
		}

		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			continue
		}
		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			continue
		}

		geoServers = append(geoServers, &geoServer{Server: s, Latitude: lat, Longitude: lon})
	}

	return geoServers
}

//https://gist.github.com/cdipaolo/d3f8db3848278b49db68
func getDistanceFrom(lat1, lon1, lat2, lon2 float64) float64 {
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}

func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

//一致性哈希选择器，基于JumpConsistentHash.
type consistentHashSelector struct {
	mu sync.Mutex
	h        *doublejump.Hash
	services map[string]Service
}

func newConsistentHashSelector(services []Service) Selector {
	ss := make(map[string]Service)
	h := doublejump.NewHash()
	for _, s := range services {
		h.Add(s.ID)
		ss[s.ID] = s
	}
	return &consistentHashSelector{services: ss, h: h, mu: sync.Mutex{}}
}

func (s consistentHashSelector) Select(ctx context.Context) Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.services
	if len(ss) == 0 {
		return Service{}
	}
	//生成hash key
	key := genKey(time.Now().String())
	id, _ := s.h.Get(key).(string)
	return s.services[id]
}

func (s *consistentHashSelector) UpdateServer(services []Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := make(map[string]Service)
	for _, si := range services {
		s.h.Add(si.ID)
		ss[si.ID] = si
	}
	s.services = ss
}

//网络质量选择器，通过ping检查质量.
type weightedICMPSelector struct {
	mu sync.Mutex
	services []*Weighted
}

func newWeightedICMPSelector(services []Service) Selector {
	ss := createICMPWeighted(services)
	return &weightedICMPSelector{services: ss, mu: sync.Mutex{}}
}

func (s weightedICMPSelector) Select(ctx context.Context) Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.services
	if len(ss) == 0 {
		return Service{}
	}
	w := nextWeighted(ss)
	if w == nil {
		return Service{}
	}
	return w.Server
}

func (s *weightedICMPSelector) UpdateServer(services []Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := createICMPWeighted(services)
	s.services = ss
}

func createICMPWeighted(services []Service) []*Weighted {
	var ss = make([]*Weighted, 0, len(services))
	for _, s := range services {
		w := &Weighted{Server: s, Weight: 1, EffectiveWeight: 1}
		rtt, _ := Ping(s.Address)
		rtt = CalculateWeight(rtt)
		w.Weight = rtt
		w.EffectiveWeight = rtt
		ss = append(ss, w)
	}
	return ss
}

// Ping gets network traffic by ICMP
func Ping(host string) (rtt int, err error) {
	rtt = 1000 //default and timeout is 1000 ms

	p := fastping.NewPinger()
	_, _ = p.Network("udp")
	ra, err := net.ResolveIPAddr("ip4:icmp", host)
	if err != nil {
		return 0, err
	}
	p.AddIPAddr(ra)

	p.OnRecv = func(addr *net.IPAddr, r time.Duration) {
		rtt = int(r.Nanoseconds() / 1000000)
	}
	// p.OnIdle = func() {

	// }
	err = p.Run()

	return rtt, err
}

// CalculateWeight converts the rtt to weighted by:
//  1. weight=191 if t <= 10
//  2. weight=201 -t if 10 < t <=200
//  3. weight=1 if 200 < t < 1000
//  4. weight = 0 if t >= 1000
//
// It means services that ping time t < 10 will be preferred
// and services won't be selected if t > 1000.
// It is hard coded based on Ops experience.
func CalculateWeight(rtt int) int {
	switch {
	case rtt >= 0 && rtt <= 10:
		return 191
	case rtt > 10 && rtt <= 200:
		return 201 - rtt
	case rtt > 100 && rtt < 1000:
		return 1
	default:
		return 0
	}
}

// Hash consistently chooses a hash bucket number in the range [0, numBuckets) for the given key. numBuckets must be >= 1.
func Hash(key uint64, buckets int32) int32 {
	if buckets <= 0 {
		buckets = 1
	}

	var b, j int64

	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

// HashString get a hash value of a string
func HashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// HashServiceAndArgs define a hash function
type HashServiceAndArgs func(len int, options ...interface{}) int

// ConsistentFunction define a hash function
// Return service address, like "tcp@127.0.0.1:8970"
type ConsistentAddrStrFunction func(options ...interface{}) string

func genKey(options ...interface{}) uint64 {
	keyString := ""
	for _, opt := range options {
		keyString = keyString + "/" + toString(opt)
	}

	return HashString(keyString)
}

// JumpConsistentHash selects a server by serviceMethod and args
func JumpConsistentHash(len int, options ...interface{}) int {
	return int(Hash(genKey(options...), int32(len)))
}

func toString(obj interface{}) string {
	return fmt.Sprintf("%v", obj)
}
