package repositorymongo

import (
	"context"
	"fmt"
	"github.com/ace-zhaoy/errors"
	goid "github.com/ace-zhaoy/go-id"
	"github.com/magiconair/properties/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"testing"
)

var (
	mongoEndpoint string
	idGen         = goid.NewID()
)

func RunWithMongoDBInContainer() (endpoint string, teardown func()) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mongo:6",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog("Waiting for connections"),
			wait.ForListeningPort("27017/tcp"),
		),
	}
	mongoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	endpoint, err = mongoC.Endpoint(ctx, "mongodb")
	if err != nil {
		log.Fatalf("failed to get endpoint: %s", err)
	}

	return endpoint, func() {
		if err := mongoC.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}
}

func TestMain(m *testing.M) {
	endpoint, teardown := RunWithMongoDBInContainer()
	defer teardown()
	mongoEndpoint = endpoint
	fmt.Println(mongoEndpoint)
	m.Run()
}

type User1 struct {
	ID        int64 `json:"id" bson:"mongo_id"`
	DeletedAt int64 `json:"deleted_at" bson:"mongo_deleted_at"`
}

func (u *User1) GetID() int64 {
	return u.ID
}

func (u *User1) SetID(id int64) {
	u.ID = id
}

type User2 struct {
	ID        int64 `json:"json_id"`
	DeletedAt int64 `json:"json_deleted_at"`
}

func (u *User2) GetID() int64 {
	return u.ID
}

func (u *User2) SetID(id int64) {
	u.ID = id
}

type User3 struct {
	ID int64 `json:"json_id"`
}

func (u *User3) GetID() int64 {
	return u.ID
}

func (u *User3) SetID(id int64) {
	u.ID = id
}

func TestCrudRepository_IDField(t *testing.T) {
	var collection *mongo.Collection
	repository1 := NewCrudRepository[int64, *User1](collection)
	assert.Equal(t, repository1.IDField(), "mongo_id")
	repository2 := NewCrudRepository[int64, *User2](collection)
	assert.Equal(t, repository2.IDField(), "json_id")
}

func TestCrudRepository_SoftDeleteField(t *testing.T) {
	var collection *mongo.Collection
	repository1 := NewCrudRepository[int64, *User1](collection)
	assert.Equal(t, repository1.SoftDeleteField(), "mongo_deleted_at")
	repository2 := NewCrudRepository[int64, *User2](collection)
	assert.Equal(t, repository2.SoftDeleteField(), "json_deleted_at")
	repository3 := NewCrudRepository[int64, *User3](collection)
	assert.Equal(t, repository3.SoftDeleteField(), "")
}

func TestCrudRepository_SoftDeleteEnabled(t *testing.T) {
	var collection *mongo.Collection
	repository1 := NewCrudRepository[int64, *User1](collection)
	assert.Equal(t, repository1.SoftDeleteEnabled(), true)
	repository2 := NewCrudRepository[int64, *User2](collection)
	assert.Equal(t, repository2.SoftDeleteEnabled(), true)
	repository3 := NewCrudRepository[int64, *User3](collection)
	assert.Equal(t, repository3.SoftDeleteEnabled(), false)
}

type User struct {
	ID   int64  `json:"id" bson:"_id"`
	Name string `json:"name" bson:"name"`
}

func (u *User) GetID() int64 {
	return u.ID
}

func (u *User) SetID(id int64) {
	u.ID = id
}

func getDatabase() (database *mongo.Database, teardown func()) {
	defer errors.Recover(func(e error) { log.Fatalf("getClient err: %+v", e) })
	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoEndpoint))
	database = mongoClient.Database("test")
	return database, func() {
		err = database.Drop(context.Background())
		errors.Check(errors.Wrap(err, "failed to drop database"))
	}
}
