package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
)

// CheckpointVersion 是续传标记的当前版本。
const CheckpointVersion = 1

// ErrCheckpointUnsupportedVersion 表示续传标记版本高于本 SDK 支持的版本。
var ErrCheckpointUnsupportedVersion = errors.New("sdk: unsupported checkpoint version")

// Checkpoint 是分页遍历的可序列化续传标记，可落盘并跨进程传递。
type Checkpoint struct {
	// Version 标记格式版本，用于版本演进时的兼容处理。
	Version int `json:"version"`
	// Cursor 下一次要拉取的游标；为空且遍历已完成时表示没有剩余数据。
	Cursor string `json:"cursor"`
	// Done 表示服务端数据已遍历完（next_cursor 为空），标记为终态。
	Done bool `json:"done,omitempty"`
	// Seen 已处理元素的去重键集合，保证续传后不重复产出（含数据变动漂移场景）。
	Seen map[string]bool `json:"seen,omitempty"`
	// Visited 已访问过的游标序列，用于检测服务端游标重复/绕回。
	Visited []string `json:"visited_cursors,omitempty"`
	// EmptyStreak 连续未产出新元素的页数（空转检测的跨进程状态）。
	EmptyStreak int `json:"empty_streak,omitempty"`
	// PagesDone / ItemsDone 已完成的页数与去重后元素数（跨进程累计）。
	PagesDone int `json:"pages_done"`
	ItemsDone int `json:"items_done"`
}

// Marshal 将续传标记序列化为可长期保存的 JSON。
//
// 可长期保存的内容：Version、Cursor、Done、Seen、Visited、EmptyStreak、
// PagesDone、ItemsDone。这些字段只包含遍历协议层面的状态，不含任何凭证、
// 连接信息或进程内对象，因此可以落盘、跨进程/跨机器传递。
// 注意：Seen 与 Visited 的规模随已处理元素数/页数增长，属预期开销。
func (c Checkpoint) Marshal() ([]byte, error) {
	c.Version = CheckpointVersion
	return json.Marshal(c)
}

// UnmarshalCheckpoint 反序列化续传标记，并做版本兼容校验：
//   - 未来版本号更高时返回 ErrCheckpointUnsupportedVersion，拒绝静默错续；
//   - 旧版本或缺少 version 字段（Version==0）按当前版本宽容接受；
//   - 未知字段一律忽略，便于新版本添加可选字段后旧 SDK 仍能读取。
func UnmarshalCheckpoint(data []byte) (Checkpoint, error) {
	var c Checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return Checkpoint{}, &Error{Kind: KindDecode, Op: "UnmarshalCheckpoint", Err: err}
	}
	if c.Version > CheckpointVersion {
		return Checkpoint{}, &Error{
			Kind: KindDecode,
			Op:   "UnmarshalCheckpoint",
			Err:  fmt.Errorf("%w: got=%d max=%d", ErrCheckpointUnsupportedVersion, c.Version, CheckpointVersion),
		}
	}
	if c.Seen == nil {
		c.Seen = map[string]bool{}
	}
	return c, nil
}
