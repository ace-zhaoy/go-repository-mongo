# 介绍
这是一个使用`go.mongodb.org/mongo-driver/mongo`作为驱动的 repository 库，支持 [github.com/ace-zhaoy/go-repository](https://github.com/ace-zhaoy/go-repository) 协议。

# 使用
```shell
go get github.com/ace-zhaoy/go-repository-mongo
```
## 快速使用
```go
package main

import (
	"context"
	"fmt"
	"github.com/ace-zhaoy/errors"
	goid "github.com/ace-zhaoy/go-id"
	repositorymongo "github.com/ace-zhaoy/go-repository-mongo"
	"github.com/ace-zhaoy/go-repository/contract"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

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

type UserRepository struct {
	contract.CrudRepository[int64, *User]
}

func NewUserRepository(db *mongo.Database) *UserRepository {
	return &UserRepository{
		CrudRepository: repositorymongo.NewCrudRepository[int64, *User](db.Collection("user")),
	}
}

func main() {
	defer errors.Recover(func(e error) { log.Fatalf("err: %+v\n", e) })
	userName := ""
	password := ""
	host := ""
	mongoURL := fmt.Sprintf("mongodb://%s:%s@%s", userName, password, host)
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	errors.Check(err)
	userRepository := NewUserRepository(client.Database("test"))
	id, err := userRepository.Create(context.Background(), &User{
		ID:   goid.GenID(),
		Name: "test",
	})
	fmt.Printf("id = %d, err = %+v\n", id, err)
}

```

## 使用软删
> 增加 DeletedAt 属性即可
```go
package main

import (
	"context"
	"fmt"
	"github.com/ace-zhaoy/errors"
	goid "github.com/ace-zhaoy/go-id"
	"github.com/ace-zhaoy/go-repository"
	repositorymongo "github.com/ace-zhaoy/go-repository-mongo"
	"github.com/ace-zhaoy/go-repository/contract"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

type User struct {
	ID        int64  `json:"id" bson:"_id"`
	Name      string `json:"name" bson:"name"`
	DeletedAt int64  `json:"deleted_at" bson:"deleted_at"`
}

func (u *User) GetID() int64 {
	return u.ID
}

func (u *User) SetID(id int64) {
	u.ID = id
}

type UserRepository struct {
	contract.CrudRepository[int64, *User]
}

func NewUserRepository(db *mongo.Database) *UserRepository {
	return &UserRepository{
		CrudRepository: repositorymongo.NewCrudRepository[int64, *User](db.Collection("user")),
	}
}

func main() {
	defer errors.Recover(func(e error) { log.Fatalf("err: %+v\n", e) })
	userName := ""
	password := ""
	host := ""
	mongoURL := fmt.Sprintf("mongodb://%s:%s@%s", userName, password, host)
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURL))
	errors.Check(err)
	userRepository := NewUserRepository(client.Database("test"))
	id, err := userRepository.Create(ctx, &User{
		ID:   goid.GenID(),
		Name: "test",
	})
	errors.Check(err)

	err = userRepository.DeleteByID(ctx, id)
	errors.Check(err)

	// 常规查找（无法找到被删除的数据）
	_, err = userRepository.FindByID(ctx, id)
	fmt.Println(errors.Is(err, repository.ErrNotFound))

	// 使用 Unscoped 查找（可以找到被删除的数据）
	user, err := userRepository.Unscoped().FindByID(ctx, id)
	errors.Check(err)
	fmt.Println(user.DeletedAt)
}

```
