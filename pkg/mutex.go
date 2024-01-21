package pkg

import "sync"

type HasLocker interface{ GetLocker() *sync.RWMutex }

func LockWrap(i HasLocker, f func()) {
	i.GetLocker().Lock()
	defer i.GetLocker().Unlock()
	f()
}

func RLockWrap(i HasLocker, f func()) {
	i.GetLocker().RLock()
	defer i.GetLocker().RUnlock()
	f()
}
