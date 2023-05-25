package dbq

import (
	"fmt"
	"strings"
	"time"
)

type MsgStatus int

const (
	MsgStatusNil MsgStatus = iota
	MsgStatusPending
	MsgStatusDispatched
	MsgStatusRunning
	MsgStatusCanceled
	MsgStatusFinished
	MsgStatusFailed
)

func (s MsgStatus) String() string {
	switch s {
	case MsgStatusPending:
		return "pending"
	case MsgStatusDispatched:
		return "dispatched"
	case MsgStatusRunning:
		return "running"
	case MsgStatusCanceled:
		return "canceled"
	case MsgStatusFinished:
		return "finished"
	case MsgStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type Msg struct {
	ID         int64     `json:"id"` // auto increment
	Data       []byte    `json:"data"`
	Status     MsgStatus `json:"status"`
	RetCode    *int      `json:"ret_code"`
	Progress   []byte    `json:"progress"`
	Ret        []byte    `json:"ret"`
	Error      []byte    `json:"error"`
	CreatedAt  time.Time `json:"created_at"`
	ScheduleAt time.Time `json:"schedule_at"`
	UpdatedAt  time.Time `json:"updated_at"`

	Q *Q `json:"-"`
}

func NewMsg(data []byte) *Msg {
	return &Msg{
		Data:   data,
		Status: MsgStatusPending,
	}
}

type Q struct {
	Name string `json:"name"`
}

func (Q) New(name string) (*Q, error) {
	q := &Q{Name: name}
	if err := q.Create(); err != nil {
		return nil, err
	}
	return q, nil
}

func (Q) Exists(name string) (bool, error) {
	stmt := "SHOW TABLES LIKE ?"
	var tableName string
	err := DB().QueryRow(stmt, "dbq_"+name).Scan(&tableName)
	if err != nil {
		return false, err
	}
	return tableName == "dbq_"+name, nil
}

func (q *Q) TableName() string {
	return "dbq_" + q.Name
}

func (q *Q) Create() error {
	stmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY NOT NULL,
			data LONGBLOB DEFAULT NULL,
			status INT NOT NULL,
			ret_code INT DEFAULT NULL,
			progress LONGBLOB DEFAULT NULL,
			ret_data LONGBLOB DEFAULT NULL,
			error_msg LONGBLOB DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			schedule_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			KEY(schedule_at),
			KEY(status)
		)`, q.TableName())
	_, err := DB().Exec(stmt)
	return err
}

func (q *Q) Drop() error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", q.TableName())
	_, err := DB().Exec(stmt)
	return err
}

func (q *Q) Clear() error {
	if err := q.Drop(); err != nil {
		return err
	}
	return q.Create()
}

func (q *Q) push(msgs []*Msg) error {
	tx, err := DB().Begin()
	if err != nil {
		tx.Rollback()
		return err
	}
	preparedStmt := fmt.Sprintf(`
		INSERT INTO %s
			(id, data, status, schedule_at, created_at, updated_at)
		VALUES
			(?, ?, ?, ?, ?, ?)
	`, q.TableName())

	stmt, err := tx.Prepare(preparedStmt)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, msg := range msgs {
		if msg.CreatedAt.IsZero() {
			msg.CreatedAt = time.Now()
		}
		if msg.UpdatedAt.IsZero() {
			msg.UpdatedAt = time.Now()
		}
		if msg.ScheduleAt.IsZero() {
			msg.ScheduleAt = time.Now()
		}
		if msg.ID == 0 {
			return fmt.Errorf("msg.ID is required")
		}

		_, err = stmt.Exec(msg.ID,
			msg.Data,
			msg.Status,
			msg.ScheduleAt,
			msg.CreatedAt,
			msg.UpdatedAt)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (q *Q) pull(limit int, dryrun bool) ([]*Msg, error) {
	tx, err := DB().Begin()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	forUpdate := ""
	if !dryrun {
		forUpdate = "FOR UPDATE"
	}

	stmt := fmt.Sprintf(`
		SELECT 
			id, data, status, ret_code, progress, ret_data, error_msg, created_at, schedule_at, updated_at
		FROM 
			%s
		WHERE 
			status = ? AND schedule_at <= ?
		ORDER BY schedule_at ASC
		LIMIT %d
		%s
		`, q.TableName(), limit, forUpdate)
	rows, err := tx.Query(stmt, MsgStatusPending, time.Now())
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	msgs := []*Msg{}
	for rows.Next() {
		msg := &Msg{}
		err := rows.Scan(
			&msg.ID,
			&msg.Data,
			&msg.Status,
			&msg.RetCode,
			&msg.Progress,
			&msg.Ret,
			&msg.Error,
			&msg.CreatedAt,
			&msg.ScheduleAt,
			&msg.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	if !dryrun {
		for _, msg := range msgs {
			stmt = fmt.Sprintf(`
			UPDATE 
				%s
			SET
				status = ?, updated_at = ?
			WHERE
				id = ?
			`, q.TableName())
			_, err = tx.Exec(stmt, MsgStatusDispatched, time.Now(), msg.ID)
			if err != nil {
				tx.Rollback()
				return nil, err
			}
			msg.Status = MsgStatusDispatched
		}
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		return msgs, nil
	}
	tx.Rollback()
	return msgs, nil
}

func (q *Q) Pull(limit int) ([]*Msg, error) {
	return q.pull(limit, false)
}

func (q *Q) Push(msgs []*Msg) error {
	return q.push(msgs)
}

func (q *Q) GetMsgByID(id int64) (*Msg, error) {
	stmt := fmt.Sprintf(`
		SELECT 
			id, data, status, ret_code, progress, ret_data, error_msg, 
			created_at, schedule_at, updated_at
		FROM %s
		WHERE 
			id = ?
		`, q.TableName())
	msg := &Msg{}
	err := DB().QueryRow(stmt, id).Scan(
		&msg.ID,
		&msg.Data,
		&msg.Status,
		&msg.RetCode,
		&msg.Progress,
		&msg.Ret,
		&msg.Error,
		&msg.CreatedAt,
		&msg.ScheduleAt,
		&msg.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

/*
func (q *Q) UpdateMsg(msg *Msg) error {
	stmt := fmt.Sprintf(`
		UPDATE
			%s
		SET
			status = ?, ret_code = ?, progress = ?, ret_data = ?, error_msg = ?, updated_at = ?
		WHERE
			id = ?
		`, q.TableName())
	_, err := DB().Exec(stmt,
		msg.Status,
		msg.RetCode,
		msg.Progress,
		msg.Ret,
		msg.Error,
		time.Now(),
		msg.ID,
	)
	if err != nil {
		return err
	}
	return nil
}
*/

func (q *Q) UpdateMsg(msg *Msg) error {
	stmt := fmt.Sprintf("UPDATE %s SET", q.TableName())

	var args []interface{}
	var updateFields []string

	if msg.RetCode != nil {
		updateFields = append(updateFields, "ret_code = ?")
		args = append(args, *msg.RetCode)
	}
	if msg.Progress != nil {
		updateFields = append(updateFields, "progress = ?")
		args = append(args, msg.Progress)
	}
	if msg.Ret != nil {
		updateFields = append(updateFields, "ret_data = ?")
		args = append(args, msg.Ret)
	}
	if msg.Error != nil {
		updateFields = append(updateFields, "error_msg = ?")
		args = append(args, msg.Error)
	}
	if msg.Status != MsgStatusNil {
		updateFields = append(updateFields, "status = ?")
		args = append(args, msg.Status)
	}

	if len(updateFields) == 0 {
		// No fields to update
		return nil
	}

	stmt += " " + strings.Join(updateFields, ", ")
	stmt += " WHERE id = ?"
	args = append(args, msg.ID)

	_, err := DB().Exec(stmt, args...)
	if err != nil {
		return err
	}
	return nil
}
