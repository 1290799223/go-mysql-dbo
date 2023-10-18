// 数据库操作包
/*
	db := NewDb("127.0.0.1", 3306, "user", "pass", "dbname").Connect()
*/
package dbo

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Db struct {
	Ip       string
	Port     int
	Username string
	Password string
	DbName   string
	Conn     *sql.DB
}
type dbRow map[string]interface{}

// 数据库操作接口
func NewDb(ip string, port int, username string, password string, dbname string) *Db {
	db := &Db{
		Ip:       ip,
		Port:     port,
		Username: username,
		Password: password,
		DbName:   dbname,
	}
	return db
}

// 连接数据库
func (this *Db) Connect() *Db {
	var err error
	this.Conn, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", this.Username, this.Password, this.Ip, this.Port, this.DbName))
	if err != nil {
		log.Fatalln("Db Connect Err:", err)
	} else {
		log.Println("Db Connect is Ok!")
	}
	return this
}

// 数据库查询接口
/*
	db.Insert("SELECT * FROM `secret`")
*/
func (this *Db) Query(sql string) map[int]map[string]string {
	rows, err := this.Conn.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	//这里表示一行所有列的值，用[]byte表示
	vals := make([][]byte, len(columns))
	//这里表示一行填充数据
	scans := make([]interface{}, len(columns))
	//这里scans引用vals，把数据填充到[]byte里
	for k, _ := range vals {
		scans[k] = &vals[k]
	}
	i := 0
	result := make(map[int]map[string]string)
	for rows.Next() {
		//填充数据
		rows.Scan(scans...)
		//每行数据
		row := make(map[string]string)
		//把vals中的数据复制到row中
		for k, v := range vals {
			key := columns[k]
			//这里把[]byte数据转成string
			row[key] = string(v)
		}
		//放入结果集
		result[i] = row
		i++
	}
	return result
}

func scanRow(rows *sql.Rows) (dbRow, error) {
	columns, _ := rows.Columns()

	vals := make([]interface{}, len(columns))
	valsPtr := make([]interface{}, len(columns))

	for i := range vals {
		valsPtr[i] = &vals[i]
	}

	err := rows.Scan(valsPtr...)

	if err != nil {
		return nil, err
	}

	r := make(dbRow)

	for i, v := range columns {
		if va, ok := vals[i].([]byte); ok {
			r[v] = string(va)
		} else {
			r[v] = vals[i]
		}
	}

	return r, nil

}

// 获取一行记录
func (this *Db) GetOne(sql string, args ...interface{}) (dbRow, error) {
	rows, err := this.Conn.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	rows.Next()
	result, err := scanRow(rows)
	return result, err
}

// 获取多行记录
func (this *Db) GetAll(sql string, args ...interface{}) ([]dbRow, error) {
	rows, err := this.Conn.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := make([]dbRow, 0)

	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			continue
		}

		result = append(result, r)
	}

	return result, nil

}

// 插入记录
/*
	使用方法
	ins := make(dbRow)
	ins["proxy"] = "1.1.1.1:8088"
	ins["id"] = "9999"
	db.Insert("secret", ins)

*/
func (this *Db) Insert(table string, data dbRow) (int64, error) {
	fields := make([]string, 0)
	vals := make([]interface{}, 0)
	placeHolder := make([]string, 0)

	for f, v := range data {
		fields = append(fields, f)
		vals = append(vals, v)
		placeHolder = append(placeHolder, "?")
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) ", table, strings.Join(fields, ","), strings.Join(placeHolder, ","))
	result, err := this.Conn.Exec(sql, vals...)
	if err != nil {
		return 0, err
	}

	lID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lID, nil
}

// 更新记录
/*
	使用方法
	upd := make(dbRow)
	upd["proxy"] = "1.1.1.1:8088"
	upd["error"] = "1"
	db.Update("secret", "id=? and error=?", upd, 1, 0)

*/
func (this *Db) Update(table, condition string, data dbRow, args ...interface{}) (int64, error) {
	params := make([]string, 0)
	vals := make([]interface{}, 0)

	for f, v := range data {
		params = append(params, f+"=?")
		vals = append(vals, v)
	}

	sql := "UPDATE %s SET %s"
	if condition != "" {
		sql += " WHERE %s"
		sql = fmt.Sprintf(sql, table, strings.Join(params, ","), condition)
		vals = append(vals, args...)
	} else {
		sql = fmt.Sprintf(sql, table, strings.Join(params, ","))
	}
	result, err := this.Conn.Exec(sql, vals...)
	if err != nil {
		return 0, err
	}

	aID, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return aID, nil
}

// 删除记录
/*
	使用方法
	db.Delete("secret", "id=?", 2)
*/
func (this *Db) Delete(table, condition string, args ...interface{}) (int64, error) {
	sql := "DELETE FROM %s "
	if condition != "" {
		sql += "WHERE %s"
		sql = fmt.Sprintf(sql, table, condition)
	} else {
		sql = fmt.Sprintf(sql, table)
	}

	result, err := this.Conn.Exec(sql, args...)
	if err != nil {
		return 0, err
	}

	aID, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return aID, nil

}
