package replicationstrategy

type Strategy interface {
	Run() error
	Stop() error
}
