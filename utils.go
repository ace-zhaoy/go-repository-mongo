package repositorymongo

import (
	"github.com/ace-zhaoy/go-repository/contract"
	"github.com/ace-zhaoy/go-utils/ucondition"
	"github.com/ace-zhaoy/go-utils/uslice"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
	"strings"
)

func getIDField(entity any) string {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic("entity must be a struct")
	}

	field, found := t.FieldByName("ID")
	if !found {
		field, found = t.FieldByName("Id")
		if !found {
			panic("entity must have field `ID` or `Id`")
		}
	}
	tag := field.Tag.Get("bson")
	if tag == "" {
		tag = field.Tag.Get("json")
	}
	if tag != "" {
		return strings.Split(tag, ",")[0]
	}

	return "_id"
}

func getDeletedAtField(entity any) string {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic("entity must be a struct")
	}

	field, found := t.FieldByName("DeletedAt")
	if !found {
		return ""
	}

	tag := field.Tag.Get("bson")
	if tag == "" {
		tag = field.Tag.Get("json")
	}
	if tag != "" {
		return strings.Split(tag, ",")[0]
	}

	return "deleted_at"
}

func OrdersToSort(orders []contract.Order) bson.D {
	return uslice.Map(orders, func(order contract.Order) bson.E {
		return bson.E{
			Key:   order.Key,
			Value: ucondition.If(order.Value < 0, -1, 1),
		}
	})
}

func getNonZeroFields(data any) bson.M {
	result := bson.M{}
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.IsZero() {
			tag := v.Type().Field(i).Tag
			fieldName := tag.Get("bson")
			if fieldName == "" {
				fieldName = tag.Get("json")
				if fieldName == "" {
					fieldName = v.Type().Field(i).Name
				}
			}
			fieldName = strings.Split(fieldName, ",")[0]
			result[fieldName] = field.Interface()
		}
	}
	return result
}
