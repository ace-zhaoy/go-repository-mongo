package repositorymongo

import (
	"context"
	"github.com/ace-zhaoy/errors"
	"github.com/ace-zhaoy/go-repository"
	"github.com/ace-zhaoy/go-repository/contract"
	"github.com/ace-zhaoy/go-utils/umap"
	"github.com/ace-zhaoy/go-utils/uslice"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type CrudRepository[ID comparable, ENTITY contract.ENTITY[ID]] struct {
	collection        *mongo.Collection
	unscoped          bool
	idField           string
	softDeleteField   string
	softDeleteEnabled bool
}

var _ contract.CrudRepository[int64, contract.ENTITY[int64]] = (*CrudRepository[int64, contract.ENTITY[int64]])(nil)

func NewCrudRepository[ID comparable, ENTITY contract.ENTITY[ID]](collection *mongo.Collection) *CrudRepository[ID, ENTITY] {
	var entity ENTITY
	softDeleteField := getDeletedAtField(entity)
	return &CrudRepository[ID, ENTITY]{
		collection:        collection,
		idField:           getIDField(entity),
		softDeleteField:   softDeleteField,
		softDeleteEnabled: softDeleteField != "",
	}
}

func (c *CrudRepository[ID, ENTITY]) clone() *CrudRepository[ID, ENTITY] {
	return &CrudRepository[ID, ENTITY]{
		collection:        c.collection,
		unscoped:          c.unscoped,
		idField:           c.idField,
		softDeleteField:   c.softDeleteField,
		softDeleteEnabled: c.softDeleteEnabled,
	}
}

func (c *CrudRepository[ID, ENTITY]) buildFilter(filter map[string]any) bson.D {
	d := bson.D{}
	umap.Foreach(filter, func(k string, v any) {
		d = append(d, bson.E{Key: k, Value: v})
	})
	if c.softDeleteEnabled && !c.unscoped {
		d = append(d, bson.E{
			Key: "$or", Value: bson.A{
				bson.M{"deleted_at": 0},
				bson.M{"deleted_at": bson.M{"$exists": false}},
			},
		})
	}

	return d
}

func (c *CrudRepository[ID, ENTITY]) IsUnscoped() bool {
	return c.unscoped
}

func (c *CrudRepository[ID, ENTITY]) Unscoped() contract.CrudRepository[ID, ENTITY] {
	cc := c.clone()
	cc.unscoped = true
	return cc
}

func (c *CrudRepository[ID, ENTITY]) IDField() string {
	return c.idField
}

func (c *CrudRepository[ID, ENTITY]) SoftDeleteField() string {
	return c.softDeleteField
}

func (c *CrudRepository[ID, ENTITY]) SoftDeleteEnabled() bool {
	return c.softDeleteEnabled
}

func (c *CrudRepository[ID, ENTITY]) Create(ctx context.Context, entity ENTITY) (id ID, err error) {
	defer errors.Recover(func(e error) { err = e })
	result, err := c.collection.InsertOne(ctx, entity)
	if err != nil && mongo.IsDuplicateKeyError(err) {
		err = repository.ErrDuplicatedKey.WrapStack(err)
	}

	errors.Check(errors.WithStack(err))
	id, ok := result.InsertedID.(ID)
	if !ok {
		errors.Check(errors.NewWithStack("unexpected type: %T", result.InsertedID))
	}
	entity.SetID(id)
	return
}

func (c *CrudRepository[ID, ENTITY]) FindOne(ctx context.Context, filter map[string]any, orders ...contract.Order) (entity ENTITY, err error) {
	defer errors.Recover(func(e error) { err = errors.Wrap(e, "param: %v, %v", filter, orders) })
	opts := options.FindOne()
	if len(orders) > 0 {
		opts.SetSort(OrdersToSort(orders))
	}
	err = c.collection.FindOne(ctx, c.buildFilter(filter), opts).Decode(&entity)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		err = repository.ErrNotFound.WrapStack(err)
	}
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) FindByID(ctx context.Context, id ID) (entity ENTITY, err error) {
	defer errors.Recover(func(e error) { err = errors.Wrap(e, "param: %v", id) })
	filter := c.buildFilter(bson.M{c.idField: id})
	err = c.collection.FindOne(ctx, filter).Decode(&entity)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		err = repository.ErrNotFound.WrapStack(err)
	}
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) FindByIDs(ctx context.Context, ids []ID) (collection contract.Collection[ID, ENTITY], err error) {
	defer errors.Recover(func(e error) { err = errors.Wrap(e, "param: %v", ids) })
	var entities []ENTITY
	if len(ids) == 0 {
		collection = repository.NewCollection[ID](entities)
		return
	}

	filter := c.buildFilter(bson.M{c.idField: bson.M{"$in": ids}})
	cursor, err := c.collection.Find(ctx, filter)
	errors.Check(errors.WithStack(err))
	err = cursor.All(ctx, &entities)
	errors.Check(errors.WithStack(err))

	collection = repository.NewCollection[ID](entities)
	return
}

func (c *CrudRepository[ID, ENTITY]) FindByPage(ctx context.Context, limit, offset int, orders ...contract.Order) (collection contract.Collection[ID, ENTITY], err error) {
	defer errors.Recover(func(e error) { err = errors.Wrap(e, "param: %v, %v, %v", limit, offset, orders) })
	opts := options.Find().SetSkip(int64(offset)).SetLimit(int64(limit))
	if len(orders) > 0 {
		opts.SetSort(OrdersToSort(orders))
	}

	filter := c.buildFilter(bson.M{})
	cursor, err := c.collection.Find(ctx, filter, opts)
	errors.Check(errors.WithStack(err))

	var entities []ENTITY
	err = cursor.All(ctx, &entities)
	errors.Check(errors.WithStack(err))

	collection = repository.NewCollection[ID](entities)
	return
}

func (c *CrudRepository[ID, ENTITY]) FindByFilter(ctx context.Context, filter map[string]any) (collection contract.Collection[ID, ENTITY], err error) {
	defer errors.Recover(func(e error) { err = e })

	cursor, err := c.collection.Find(ctx, c.buildFilter(filter))
	errors.Check(errors.WithStack(err))

	var entities []ENTITY
	err = cursor.All(ctx, &entities)
	errors.Check(errors.WithStack(err))

	collection = repository.NewCollection[ID](entities)
	return
}

func (c *CrudRepository[ID, ENTITY]) FindByFilterWithPage(ctx context.Context, filter map[string]any, limit, offset int, orders ...contract.Order) (collection contract.Collection[ID, ENTITY], err error) {
	defer errors.Recover(func(e error) { err = e })

	opts := options.Find().SetSkip(int64(offset)).SetLimit(int64(limit))
	if len(orders) > 0 {
		opts.SetSort(OrdersToSort(orders))
	}

	cursor, err := c.collection.Find(ctx, c.buildFilter(filter), opts)
	errors.Check(errors.WithStack(err))

	var entities []ENTITY
	err = cursor.All(ctx, &entities)
	errors.Check(errors.WithStack(err))

	collection = repository.NewCollection[ID](entities)
	return
}

func (c *CrudRepository[ID, ENTITY]) FindAll(ctx context.Context) (collection contract.Collection[ID, ENTITY], err error) {
	defer errors.Recover(func(e error) { err = e })
	cursor, err := c.collection.Find(ctx, c.buildFilter(bson.M{}))
	errors.Check(errors.WithStack(err))

	var entities []ENTITY
	err = cursor.All(ctx, &entities)
	errors.Check(errors.WithStack(err))

	collection = repository.NewCollection[ID](entities)
	return
}

func (c *CrudRepository[ID, ENTITY]) Count(ctx context.Context) (count int, err error) {
	defer errors.Recover(func(e error) { err = e })
	cnt, err := c.collection.CountDocuments(ctx, c.buildFilter(bson.M{}))
	errors.Check(errors.WithStack(err))
	count = int(cnt)
	return
}

func (c *CrudRepository[ID, ENTITY]) CountByFilter(ctx context.Context, filter map[string]any) (count int, err error) {
	defer errors.Recover(func(e error) { err = e })
	cnt, err := c.collection.CountDocuments(ctx, c.buildFilter(filter))
	errors.Check(errors.WithStack(err))
	count = int(cnt)
	return
}

func (c *CrudRepository[ID, ENTITY]) Exists(ctx context.Context, filter map[string]any) (exists bool, err error) {
	defer errors.Recover(func(e error) { err = e })

	opts := options.FindOne().SetProjection(bson.D{{c.idField, 1}})
	err = c.collection.FindOne(ctx, c.buildFilter(filter), opts).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	errors.Check(errors.WithStack(err))
	return true, nil
}

func (c *CrudRepository[ID, ENTITY]) ExistsByID(ctx context.Context, id ID) (exists bool, err error) {
	defer errors.Recover(func(e error) { err = e })
	filter := c.buildFilter(bson.M{c.idField: id})
	opts := options.FindOne().SetProjection(bson.D{{c.idField, 1}})
	err = c.collection.FindOne(ctx, filter, opts).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	errors.Check(errors.WithStack(err))
	return true, nil
}

func (c *CrudRepository[ID, ENTITY]) ExistsByIDs(ctx context.Context, ids []ID) (exists contract.Dict[ID, bool], err error) {
	defer errors.Recover(func(e error) { err = e })
	if len(ids) == 0 {
		exists = repository.NewDict[ID, bool](nil)
		return
	}

	filter := c.buildFilter(bson.M{c.idField: bson.M{"$in": ids}})
	opts := options.Find().SetProjection(bson.D{{c.idField, 1}})
	cursor, err := c.collection.Find(ctx, filter, opts)
	errors.Check(errors.WithStack(err))

	var entities []ENTITY
	err = cursor.All(ctx, &entities)
	errors.Check(errors.WithStack(err))

	exists = repository.NewDictWithSize[ID, bool](len(entities))
	uslice.ForEach(entities, func(item ENTITY) {
		exists.Set(item.GetID(), true)
	})

	return
}

func (c *CrudRepository[ID, ENTITY]) Update(ctx context.Context, filter map[string]any, data map[string]any) (err error) {
	defer errors.Recover(func(e error) { err = e })
	_, err = c.collection.UpdateMany(ctx, c.buildFilter(filter), bson.M{"$set": data})
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) UpdateByID(ctx context.Context, id ID, data map[string]any) (err error) {
	defer errors.Recover(func(e error) { err = e })
	_, err = c.collection.UpdateOne(ctx, c.buildFilter(bson.M{c.idField: id}), bson.M{"$set": data})
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) UpdateNonZero(ctx context.Context, filter map[string]any, entity ENTITY) (err error) {
	defer errors.Recover(func(e error) { err = e })
	data := getNonZeroFields(entity)
	if len(data) == 0 {
		return
	}

	_, err = c.collection.UpdateMany(ctx, c.buildFilter(filter), bson.M{"$set": data})
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) UpdateNonZeroByID(ctx context.Context, id ID, entity ENTITY) (err error) {
	defer errors.Recover(func(e error) { err = e })
	data := getNonZeroFields(entity)
	if len(data) == 0 {
		return
	}

	_, err = c.collection.UpdateOne(ctx, c.buildFilter(bson.M{c.idField: id}), bson.M{"$set": data})
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) softDelete(ctx context.Context, filter map[string]any) (err error) {
	defer errors.Recover(func(e error) { err = e })
	err = c.Update(ctx, filter, bson.M{c.softDeleteField: time.Now().Unix()})
	errors.Check(err)
	return
}

func (c *CrudRepository[ID, ENTITY]) Delete(ctx context.Context, filter map[string]any) (err error) {
	defer errors.Recover(func(e error) { err = e })
	if c.softDeleteEnabled && !c.unscoped {
		errors.Check(c.softDelete(ctx, filter))
		return
	}
	_, err = c.collection.DeleteMany(ctx, filter)
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) DeleteByID(ctx context.Context, id ID) (err error) {
	defer errors.Recover(func(e error) { err = e })
	filter := bson.M{c.idField: id}
	if c.softDeleteEnabled && !c.unscoped {
		errors.Check(c.softDelete(ctx, filter))
		return
	}
	_, err = c.collection.DeleteOne(ctx, filter)
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) DeleteByIDs(ctx context.Context, ids []ID) (err error) {
	defer errors.Recover(func(e error) { err = e })
	if len(ids) == 0 {
		return
	}
	filter := bson.M{c.idField: bson.M{"$in": ids}}
	if c.softDeleteEnabled && !c.unscoped {
		errors.Check(c.softDelete(ctx, filter))
		return
	}
	_, err = c.collection.DeleteMany(ctx, filter)
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) DeleteAll(ctx context.Context) (err error) {
	defer errors.Recover(func(e error) { err = e })
	filter := bson.M{}
	if c.softDeleteEnabled && !c.unscoped {
		errors.Check(c.softDelete(ctx, filter))
		return
	}
	_, err = c.collection.DeleteMany(ctx, filter)
	errors.Check(errors.WithStack(err))
	return
}

func (c *CrudRepository[ID, ENTITY]) DeleteAllByFilter(ctx context.Context, filter map[string]any) (err error) {
	defer errors.Recover(func(e error) { err = e })
	if c.softDeleteEnabled && !c.unscoped {
		errors.Check(c.softDelete(ctx, filter))
		return
	}
	_, err = c.collection.DeleteMany(ctx, filter)
	errors.Check(errors.WithStack(err))
	return
}
