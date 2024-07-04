package repositorymongo

import (
	"context"
	"github.com/ace-zhaoy/errors"
	"github.com/ace-zhaoy/go-repository"
	"github.com/ace-zhaoy/go-repository/contract"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
)

// table: users
type UserRepository struct {
	contract.CrudRepository[int64, *User]
}

func NewUserRepository(db *mongo.Database) *UserRepository {
	return &UserRepository{
		CrudRepository: NewCrudRepository[int64, *User](db.Collection("users")),
	}
}

func ExampleCrudRepository() {
	defer errors.Recover(func(e error) { log.Fatalf("%+v", e) })
	var ctx = context.Background()
	var db *mongo.Database
	userRepository := NewUserRepository(db)
	id, err := userRepository.Create(ctx, &User{
		ID:   idGen.Generate(),
		Name: "test",
	})
	errors.Check(err)
	user, err := userRepository.FindByID(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		// TODO: handle not found
		return
	}
	errors.Check(err)
	_ = user
}

type Role struct {
	ID        int64  `json:"id" bson:"_id"`
	Name      string `json:"name" bson:"name"`
	DeletedAt int64  `json:"deleted_at" bson:"deleted_at"`
}

func (u *Role) GetID() int64 {
	return u.ID
}

func (u *Role) SetID(id int64) {
	u.ID = id
}

// table: role
type RoleRepository struct {
	contract.CrudRepository[int64, *Role]
}

func NewRoleRepository(db *mongo.Database) *RoleRepository {
	return &RoleRepository{
		CrudRepository: NewCrudRepository[int64, *Role](db.Collection("role")),
	}
}

func ExampleCrudRepository_SoftDelete() {
	defer errors.Recover(func(e error) { log.Fatalf("%+v", e) })
	var ctx = context.Background()
	var db *mongo.Database
	roleRepository := NewRoleRepository(db)
	id, err := roleRepository.Create(ctx, &Role{
		ID:   idGen.Generate(),
		Name: "test",
	})
	errors.Check(err)
	err = roleRepository.DeleteByID(ctx, id)
	errors.Check(err)

	// Find deleted data
	role, err := roleRepository.Unscoped().FindByID(ctx, id)
	// role exists, and DeletedAt > 0
	_ = role
}
