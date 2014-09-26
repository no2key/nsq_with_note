package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

// 任何结构组合了WaitGroupWrapper之后会拥有Wrap函数。该函数用于需要等待多个操作全部完成后才继续执行代码的场景。