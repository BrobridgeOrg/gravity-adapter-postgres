module git.brobridge.com/gravity/gravity-adapter-postgres

go 1.15

require (
	github.com/BrobridgeOrg/broton v0.0.7
	github.com/BrobridgeOrg/gravity-sdk v0.0.31
	github.com/cfsghost/parallel-chunked-flow v0.0.6
	github.com/jmoiron/sqlx v1.2.0
	github.com/json-iterator/go v1.1.10
	github.com/lib/pq v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
)

//replace github.com/BrobridgeOrg/gravity-sdk => ./gravity-sdk

//replace github.com/cfsghost/parallel-chunked-flow => ./parallel-chunked-flow
