// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

const (
	Get              = "get"
	Create           = "create"
	Set              = "set"
	Update           = "update"
	Delete           = "delete"
	CompareAndSwap   = "compareAndSwap"
	CompareAndDelete = "compareAndDelete"
	Expire           = "expire"
)

type Event struct {
	Action    string      `json:"action"`
	Node      *NodeExtern `json:"node,omitempty"`
	PrevNode  *NodeExtern `json:"prevNode,omitempty"` // 更新事情中记录之前的NodeExtern实例
	EtcdIndex uint64      `json:"-"`                  // 记录Event之后的CurrentIndex
	Refresh   bool        `json:"refresh,omitempty"`  // 表示Event只刷新了过期时间等信息，没有更新Value等，不触发watcher
}

func newEvent(action string, key string, modifiedIndex, createdIndex uint64) *Event {
	n := &NodeExtern{
		Key:           key,
		ModifiedIndex: modifiedIndex,
		CreatedIndex:  createdIndex,
	}

	return &Event{
		Action: action,
		Node:   n,
	}
}

func (e *Event) IsCreated() bool {
	if e.Action == Create {
		return true
	}
	return e.Action == Set && e.PrevNode == nil
}

func (e *Event) Index() uint64 {
	return e.Node.ModifiedIndex
}

func (e *Event) Clone() *Event {
	return &Event{
		Action:    e.Action,
		EtcdIndex: e.EtcdIndex,
		Node:      e.Node.Clone(),
		PrevNode:  e.PrevNode.Clone(),
	}
}

func (e *Event) SetRefresh() {
	e.Refresh = true
}
