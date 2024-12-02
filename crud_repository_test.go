package repositorymongo

import (
	"context"
	"fmt"
	"github.com/ace-zhaoy/errors"
	goid "github.com/ace-zhaoy/go-id"
	"github.com/ace-zhaoy/go-repository"
	"github.com/ace-zhaoy/go-repository/contract"
	"github.com/magiconair/properties/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
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
	errors.Check(errors.WithStack(err))
	database = mongoClient.Database("test")
	return database, func() {
		err = database.Drop(context.Background())
		errors.Check(errors.Wrap(err, "failed to drop database"))
	}
}

func TestCrudRepository_Create(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_Create err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	id, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	assert.Equal(t, id, user.ID)
}

func TestCrudRepository_Create_DuplicateKey(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_Create_DuplicateKey err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	_, err = userRepository.Create(context.Background(), &user)
	assert.Equal(t, errors.Is(err, repository.ErrDuplicatedKey), true)
}

func TestCrudRepository_FindOne(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindOne err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))

	foundUser, err := userRepository.FindOne(context.Background(), map[string]any{
		"name": "test",
	})
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, foundUser.Name, user.Name)

	_, err = userRepository.FindOne(context.Background(), map[string]any{
		"name": "nonexistent",
	})
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)
}

func TestCrudRepository_FindOne_WithOrder(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindOne_WithOrder err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user1 := User{
		ID:   idGen.Generate(),
		Name: "test1",
	}
	_, err := userRepository.Create(context.Background(), &user1)
	errors.Check(errors.Wrap(err, "failed to create user1"))

	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user2"))

	foundUser, err := userRepository.FindOne(context.Background(), map[string]any{
		"name": bson.M{"$regex": "test"},
	}, contract.Order{
		Key:   "name",
		Value: -1,
	})
	errors.Check(errors.Wrap(err, "failed to find user with order"))
	assert.Equal(t, foundUser.Name, user2.Name)

	foundUser, err = userRepository.FindOne(context.Background(), map[string]any{
		"name": bson.M{"$regex": "test"},
	}, contract.Order{
		Key:   "name",
		Value: 1,
	})
	errors.Check(errors.Wrap(err, "failed to find user with order"))
	assert.Equal(t, foundUser.Name, user1.Name)
}

func TestCrudRepository_FindByID(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindByID err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))
	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user2.Name, user.Name)
}

func TestCrudRepository_FindByID_NotFound(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindByID_NotFound err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))
	_, err := userRepository.FindByID(context.Background(), 1)
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)
}

func TestCrudRepository_FindByIDs(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindByIDs err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))
	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	collection, err := userRepository.FindByIDs(context.Background(), []int64{user.ID, user2.ID})
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection.Has(user.ID), true)
	assert.Equal(t, collection.Has(user2.ID), true)
	assert.Equal(t, collection.Has(3), false)
	assert.Equal(t, collection.Count(), 2)

	dict := collection.ToDict()
	assert.Equal(t, dict.Value(user.ID).Name, user.Name)
	assert.Equal(t, dict.Value(user2.ID).Name, user2.Name)
}

func TestCrudRepository_FindByPage(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindByPage err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))
	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	collection, err := userRepository.FindByPage(context.Background(), 1, 1)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection.Has(user.ID), false)
	assert.Equal(t, collection.Has(user2.ID), true)
	assert.Equal(t, collection.Count(), 1)

	collection2, err := userRepository.FindByPage(context.Background(), 1, 0, contract.Order{
		Key:   userRepository.IDField(),
		Value: -1,
	})
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection2.Has(user.ID), false)
	assert.Equal(t, collection2.Has(user2.ID), true)
	assert.Equal(t, collection2.Count(), 1)
}

func TestCrudRepository_FindByFilter(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindByFilter err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))
	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	collection, err := userRepository.FindByFilter(context.Background(), map[string]any{
		"name": "test",
	})
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection.Has(user.ID), true)
	assert.Equal(t, collection.Has(user2.ID), false)
	assert.Equal(t, collection.Count(), 1)

	collection2, err := userRepository.FindByFilter(context.Background(), map[string]any{
		"name": "test3",
	})
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection2.Count(), 0)
}

func TestCrudRepository_FindByFilterWithPage(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindByFilterWithPage err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))
	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user3 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user3)
	errors.Check(errors.Wrap(err, "failed to create user"))

	collection, err := userRepository.FindByFilterWithPage(context.Background(), map[string]any{
		"name": "test",
	}, 1, 1)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection.Has(user.ID), false)
	assert.Equal(t, collection.Has(user2.ID), true)
	assert.Equal(t, collection.Count(), 1)

	collection2, err := userRepository.FindByFilterWithPage(context.Background(), map[string]any{
		"name": "test",
	}, 1, 1, contract.Order{
		Key:   userRepository.IDField(),
		Value: -1,
	})
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection2.Has(user.ID), true)
	assert.Equal(t, collection2.Has(user2.ID), false)
	assert.Equal(t, collection2.Count(), 1)
}

func TestCrudRepository_FindAll(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_FindAll err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	collection, err := userRepository.FindAll(context.Background())
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection.Count(), 0)

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	collection1, err := userRepository.FindAll(context.Background())
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, collection1.Has(user.ID), true)
	assert.Equal(t, collection1.Has(user2.ID), true)
	assert.Equal(t, collection1.Count(), 2)
}

func TestCrudRepository_Count(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_Count err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	cnt, err := userRepository.Count(context.Background())
	errors.Check(errors.Wrap(err, "failed to count user"))
	assert.Equal(t, cnt, 0)

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	cnt, err = userRepository.Count(context.Background())
	errors.Check(errors.Wrap(err, "failed to count user"))
	assert.Equal(t, cnt, 2)
}

func TestCrudRepository_CountByFilter(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_CountByFilter err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	cnt, err := userRepository.CountByFilter(context.Background(), map[string]any{
		"name": "test",
	})
	errors.Check(errors.Wrap(err, "failed to count user"))
	assert.Equal(t, cnt, 0)

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user2 := User{
		ID:   idGen.Generate(),
		Name: "test2",
	}
	_, err = userRepository.Create(context.Background(), &user2)
	errors.Check(errors.Wrap(err, "failed to create user"))
	cnt, err = userRepository.CountByFilter(context.Background(), map[string]any{
		"name": "test",
	})
	errors.Check(errors.Wrap(err, "failed to count user"))
	assert.Equal(t, cnt, 1)
}

func TestCrudRepository_Exists(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_Exists err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	exists, err := userRepository.Exists(context.Background(), map[string]any{
		"name": "test",
	})
	errors.Check(errors.Wrap(err, "failed to check user exists"))
	assert.Equal(t, exists, false)

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	exists, err = userRepository.Exists(context.Background(), map[string]any{
		"name": "test",
	})
	errors.Check(errors.Wrap(err, "failed to check user exists"))
	assert.Equal(t, exists, true)
}

func TestCrudRepository_ExistsByID(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_ExistsByID err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	id := idGen.Generate()
	exists, err := userRepository.ExistsByID(context.Background(), id)
	errors.Check(errors.Wrap(err, "failed to check user exists"))
	assert.Equal(t, exists, false)

	user := User{
		ID:   id,
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	exists, err = userRepository.ExistsByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to check user exists"))
	assert.Equal(t, exists, true)
}

func TestCrudRepository_ExistsByIDs(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_ExistsByIDs err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	id := idGen.Generate()
	exists, err := userRepository.ExistsByIDs(context.Background(), []int64{user.ID, id})
	errors.Check(errors.Wrap(err, "failed to check user exists"))
	_, ok1 := exists.Get(user.ID)
	assert.Equal(t, ok1, true)
	_, ok2 := exists.Get(id)
	assert.Equal(t, ok2, false)
}

func TestCrudRepository_Update(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_Update err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user1, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user1.Name, user.Name)

	err = userRepository.Update(context.Background(), map[string]any{
		"name": user1.Name,
	}, map[string]any{
		"name": "test2",
	})
	errors.Check(errors.Wrap(err, "failed to update user"))
	user2, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user2.Name, "test2")
}

func TestCrudRepository_UpdateByID(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_UpdateByID err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user1, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user1.Name, user.Name)

	err = userRepository.UpdateByID(context.Background(), user.ID, map[string]any{
		"name": "test2",
	})
	errors.Check(errors.Wrap(err, "failed to update user"))
	user2, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user2.Name, "test2")
}

func TestCrudRepository_UpdateNonZero(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_UpdateNonZero err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user1, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user1.Name, user.Name)

	user1.Name = ""
	err = userRepository.UpdateNonZero(context.Background(), map[string]any{
		"name": user.Name,
	}, user1)
	errors.Check(errors.Wrap(err, "failed to update user"))
	user2, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user2.Name, "test")

	user2.Name = "test2"
	err = userRepository.UpdateNonZero(context.Background(), map[string]any{
		"name": user.Name,
	}, user2)
	errors.Check(errors.Wrap(err, "failed to update user"))
	user3, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user3.Name, "test2")
}

func TestCrudRepository_UpdateNonZeroByID(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_UpdateNonZeroByID err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user1, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user1.Name, user.Name)

	user1.Name = ""
	err = userRepository.UpdateNonZeroByID(context.Background(), user1.ID, user1)
	errors.Check(errors.Wrap(err, "failed to update user"))
	user2, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user2.Name, "test")

	user2.Name = "test2"
	err = userRepository.UpdateNonZeroByID(context.Background(), user2.ID, user2)
	errors.Check(errors.Wrap(err, "failed to update user"))
	user3, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user3.Name, "test2")
}

func TestCrudRepository_Delete(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_Delete err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	err = userRepository.Delete(context.Background(), map[string]any{
		"name": user.Name,
	})
	errors.Check(errors.Wrap(err, "failed to delete user"))
	_, err = userRepository.FindByID(context.Background(), user.ID)
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)
}

func TestCrudRepository_DeleteByID(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_DeleteByID err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	err = userRepository.DeleteByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to delete user"))
	_, err = userRepository.FindByID(context.Background(), user.ID)
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)
}

func TestCrudRepository_DeleteByIDs(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_DeleteByIDs err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))
	user1 := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user1)
	errors.Check(errors.Wrap(err, "failed to create user"))

	err = userRepository.DeleteByIDs(context.Background(), []int64{user.ID, user1.ID})
	errors.Check(errors.Wrap(err, "failed to delete user"))
	_, err = userRepository.FindByID(context.Background(), user.ID)
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)
	_, err = userRepository.FindByID(context.Background(), user1.ID)
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)
}

func TestCrudRepository_DeleteAll(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_DeleteAll err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *User](db.Collection("user"))

	user := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))

	user1 := User{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err = userRepository.Create(context.Background(), &user1)
	errors.Check(errors.Wrap(err, "failed to create user"))
	cnt, err := userRepository.Count(context.Background())
	errors.Check(errors.Wrap(err, "failed to count user"))
	assert.Equal(t, cnt, 2)

	err = userRepository.DeleteAll(context.Background())
	errors.Check(errors.Wrap(err, "failed to delete user"))

	cnt, err = userRepository.Count(context.Background())
	errors.Check(errors.Wrap(err, "failed to count user"))
	assert.Equal(t, cnt, 0)
}

type UserSoftDelete struct {
	ID        int64  `json:"id"  bson:"_id"`
	Name      string `json:"name" bson:"name"`
	DeletedAt int64  `json:"deleted_at" bson:"deleted_at"`
}

func (u *UserSoftDelete) GetID() int64 {
	return u.ID
}

func (u *UserSoftDelete) SetID(id int64) {
	u.ID = id
}

func TestCrudRepository_SoftDelete(t *testing.T) {
	defer errors.Recover(func(e error) { log.Fatalf("TestCrudRepository_SoftDelete err: %+v", e) })
	db, teardown := getDatabase()
	defer teardown()
	userRepository := NewCrudRepository[int64, *UserSoftDelete](db.Collection("user"))
	user := UserSoftDelete{
		ID:   idGen.Generate(),
		Name: "test",
	}
	_, err := userRepository.Create(context.Background(), &user)
	errors.Check(errors.Wrap(err, "failed to create user"))

	user1, err := userRepository.FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user1.Name, user.Name)
	assert.Equal(t, user1.DeletedAt == 0, true)

	err = userRepository.DeleteByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to soft delete user"))

	_, err = userRepository.FindByID(context.Background(), user.ID)
	assert.Equal(t, errors.Is(err, repository.ErrNotFound), true)

	user2, err := userRepository.Unscoped().FindByID(context.Background(), user.ID)
	errors.Check(errors.Wrap(err, "failed to find user"))
	assert.Equal(t, user2.DeletedAt > 0, true)
}
