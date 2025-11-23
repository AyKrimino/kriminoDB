package store

type Replicator interface {
	Replicate(key string, dataValue DataValue)
}
