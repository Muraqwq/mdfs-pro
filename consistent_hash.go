package main

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// HashRing 一致性哈希环
type HashRing struct {
	nodes    []int          // 已排序的哈希值切片
	nodeMap  map[int]string // 哈希值 -> 节点地址的映射
	replicas int            // 虚拟节点倍数（用于数据分布更均匀）
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodeMap:  make(map[int]string),
		replicas: replicas,
	}
}

// AddNode 向环中添加物理节点
func (h *HashRing) AddNode(address string) {
	for i := 0; i < h.replicas; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(strconv.Itoa(i) + address)))
		h.nodes = append(h.nodes, hash)
		h.nodeMap[hash] = address
	}
	sort.Ints(h.nodes) // 保持环是有序的
}

// GetNode 根据电影名获取应该存放的节点地址
func (h *HashRing) GetNode(movieName string) string {
	if len(h.nodes) == 0 {
		return ""
	}
	hash := int(crc32.ChecksumIEEE([]byte(movieName)))

	// 在环上顺时针找到第一个大于等于该哈希值的节点
	idx := sort.Search(len(h.nodes), func(i int) bool {
		return h.nodes[i] >= hash
	})

	// 如果到了环的末尾，则返回第一个节点（闭环）
	if idx == len(h.nodes) {
		idx = 0
	}

	return h.nodeMap[h.nodes[idx]]
}
