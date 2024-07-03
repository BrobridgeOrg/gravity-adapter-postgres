module git.brobridge.com/gravity/gravity-adapter-postgres

go 1.15

require (
	github.com/BrobridgeOrg/broton v0.0.7
	github.com/BrobridgeOrg/gravity-sdk v1.0.12
	github.com/cfsghost/parallel-chunked-flow v0.0.6
	github.com/jmoiron/sqlx v1.3.4
	github.com/json-iterator/go v1.1.10
	github.com/lib/pq v1.10.1
	github.com/nats-io/nats.go v1.16.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.8.0
)

//replace github.com/BrobridgeOrg/gravity-sdk => ./gravity-sdk

//replace github.com/cfsghost/parallel-chunked-flow => ./parallel-chunked-flow
