package sequence

import (
	"database/sql"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// 建立mysql链接执行REPLACE INTO语句
// REPLACE INTO sequence (stub) VALUES ('a');
// SELECT LAST_INSERT_ID();

// stub列总结值为'一下a'，这 的记录个。如果没有 SQL符合
// 条件的语句记录会检查，则会 插入sequence一 表中是否已经条有一条新的记录，其主记录键或唯一，
// 其中 索stub引 列的值列为 'a'的值与。待插入记录相同。如果有，就删除旧记录并插入新记录；如果没有，则直接插入新记录，stub列的值为'a'`
const sqlReplaceIntoStub = `REPLACE INTO sequence (stub) VALUES ('a')`

type MySQL struct {
	conn sqlx.SqlConn
}

func NewMySQL(dsn string) Sequence {
	return &MySQL{
		conn: sqlx.NewMysql(dsn),
	}
}

// Next 取下个号  mysql 实现取号器
func (m *MySQL) Next() (res uint64, err error) {
	//prepare预编译
	var stmt sqlx.StmtSession
	stmt, err = m.conn.Prepare(sqlReplaceIntoStub)
	if err != nil {
		logx.Errorw("conn.Prepare failed", logx.LogField{Key: "err", Value: err.Error()})
		return 0, err
	}
	defer stmt.Close()
	//执行sql语句
	var result sql.Result
	result, err = stmt.Exec()
	if err != nil {
		logx.Errorw("stmt.Exec failed", logx.LogField{Key: "err", Value: err.Error()})
		return 0, err
	}
	//获取刚插入的主键id
	var id int64
	id, err = result.LastInsertId()
	if err != nil {
		logx.Errorw("result.LastInsertId failed", logx.LogField{Key: "err", Value: err.Error()})
		return 0, err
	}
	return uint64(id), nil
}
